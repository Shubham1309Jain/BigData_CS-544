from kafka import KafkaProducer, KafkaAdminClient, KafkaConsumer
from kafka.admin import NewTopic
from kafka.errors import UnknownTopicOrPartitionError, TopicAlreadyExistsError
import time
import os
from sqlalchemy import create_engine, text
import report_pb2


broker= "localhost:9092"
project = os.environ.get('PROJECT', 'p7')
db_connect_url = f"mysql+mysqlconnector://root:abc@{project}-mysql-1:3306/CS544"

def Topic_Creation():
    admin = KafkaAdminClient(bootstrap_servers=broker)
    # admin.list_topics()
    try:
        admin.delete_topics(["temperatures"])
        print("Deleted existing topic 'temperatures'")
        time.sleep(3)
    except UnknownTopicOrPartitionError:
        print("Topic temperatures doesnot exists! Create a New One")
    
    try:
        admin.create_topics([NewTopic("temperatures", num_partitions=4, replication_factor=1)])
        print("Success")
    except TopicAlreadyExistsError:
        print("already exists")
    finally:  
        admin.close()


def Temp_producer():
    prod = KafkaProducer(bootstrap_servers=[broker],
                             retries=10, acks="all")
    return prod


def rs_weather(producer):
    engine = create_engine(db_connect_url)
    connection = engine.connect()
    
    last_id = 0
    
    while True:
        # Get new records since last_id
        query = text(f"SELECT * FROM temperatures WHERE id > {last_id} ORDER BY id")
        result = connection.execute(query)
        rows = result.fetchall()
        
        if rows:
            last_id = rows[-1][0]
            
            for row in rows:
                report = report_pb2.Report()
                report.date = row[2].strftime("%Y-%m-%d")
                report.degrees = row[3]
                report.station_id = row[1]
                
                # Send to Kafka
                producer.send("temperatures", 
                    key=str(row[1]).encode('utf-8'),  # station_id as key
                    value=report.SerializeToString()
                )
        
        connection.commit()
        time.sleep(0.1)


def main():
    Topic_Creation()
    producer = Temp_producer()
    rs_weather(producer)

if __name__ == "__main__":
    main()