from kafka import KafkaConsumer
import report_pb2

broker = "localhost:9092"

def main():    
    consumer = KafkaConsumer(bootstrap_servers=[broker], group_id='debug', auto_offset_reset='latest', enable_auto_commit=True)
    consumer.subscribe(["temperatures"]) # pattern=r".*_nums$"

    while True:
        batch = consumer.poll(1000)
        for topic_partition, messages in batch.items():
            for msg in messages:
                # Parse and print each message (like notebook Out [52])
                report = report_pb2.Report()
                report.ParseFromString(msg.value)
                
                print({
                    "station_id": report.station_id,
                    "date": report.date,
                    "degrees": report.degrees,
                    "partition": msg.partition
                })

if __name__ == "__main__":
    main()