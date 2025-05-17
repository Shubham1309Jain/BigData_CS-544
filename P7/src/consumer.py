import os, sys
from kafka import KafkaConsumer, TopicPartition
from subprocess import check_output
import report_pb2
import pyarrow.fs as fs
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import json
import time


os.environ["CLASSPATH"] = str(check_output([os.environ["HADOOP_HOME"]+"/bin/hdfs", "classpath", "--glob"]), "utf-8")

broker     = 'localhost:9092'
topic_name = 'temperatures'

def write_local_checkpoint(partition_id, batch_id, offset):
    info = {
        "batch_id": batch_id,
        "offset": offset
    }
    checkpoint_file = f"/partition-{partition_id}.json"
    temp_file = f"{checkpoint_file}.tmp"
    
    with open(temp_file, 'w') as f:
        json.dump(info, f)

    os.rename(temp_file, checkpoint_file)

def checkpoint(partition_id):
    checkpoint_path = f'/partition-{partition_id}.json'
    try:
        with open(checkpoint_path, 'r') as f:
            checkpoint_data = json.load(f)
            return checkpoint_data.get('batch_id', -1), checkpoint_data.get('offset', 0)
    except FileNotFoundError:
        return -1, 0

def main():
    if len(sys.argv) != 2:
        print("Usage: python consumer.py <partition_number>")
        sys.exit(1)
    
    partition_id = int(sys.argv[1])


    consumer = KafkaConsumer(
        bootstrap_servers=[broker],
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        max_poll_records=10
    )
    partition = TopicPartition(topic_name, partition_id)
    consumer.assign([partition])


    batch_id, offset= checkpoint(partition_id)
    
    consumer.seek(partition, offset)
    hdfs = fs.HadoopFileSystem("boss", 9000)
    try:
        if not hdfs.exists("/data"):
            hdfs.create_dir("/data")
    except Exception as e:
        print(f"Error creating directory: {e}")
    batch_id += 1 

    while True:
        try:
            batch = consumer.poll(1000, max_records=10)
            data=[]
            if batch:
                for topic_partition, messages in batch.items():
                    if messages:
                        for msg in messages:
                            report = report_pb2.Report()
                            report.ParseFromString(msg.value)
                            data.append({
                                'station_id': report.station_id,
                                'date': report.date,
                                'degrees': report.degrees
                            })

            if data:
                table = pd.DataFrame(data)

                path = f"/data/partition-{partition_id}-batch-{batch_id}.parquet"
                path_tmp = f"{path}.tmp"
                
                try:
                    with hdfs.open_output_stream(path_tmp) as f:
                        pq.write_table(pa.Table.from_pandas(table), f)
                    
                    hdfs.move(path_tmp, path)
                
                except Exception as e:
                    print(f"Error writing to HDFS: {e}")

                cd=consumer.position(partition)

                write_local_checkpoint(partition_id, batch_id, cd)
                batch_id +=1
            time.sleep(0.1)    
        except Exception as e:
            print(f"Error in consumer loop: {e}")
            time.sleep(1)

if __name__ == '__main__':
    main()


