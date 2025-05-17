import grpc
from concurrent import futures
import station_pb2
import station_pb2_grpc
from cassandra.cluster import Cluster
from cassandra.query import ConsistencyLevel
from pyspark.sql import SparkSession
import os
import cassandra

class StationService(station_pb2_grpc.StationServicer):
    def __init__(self):
        try:
            project = os.environ.get('PROJECT', 'p6')  # Default to 'p6' if not set
            print(f"Initializing Cassandra connection to {project}-db-[1-3]")
            
            # Add connection timeout and retry logic
            self.cluster = Cluster(
                [f"{project}-db-1", f"{project}-db-2", f"{project}-db-3"],
                connect_timeout=60,
                protocol_version=4
            )
            
            print("Establishing Cassandra session...")
            self.session = self.cluster.connect()
            
            # Verify connection works
            print(f"Connected to cluster: {self.cluster.metadata.cluster_name}")
            print(f"Hosts: {[h.address for h in self.cluster.metadata.all_hosts()]}")
            
            # Schema creation with verification
            print("\nCreating schema...")
            self.session.execute("""
                CREATE KEYSPACE IF NOT EXISTS weather 
                WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}
            """)
            print("→ Created keyspace")
            
            self.session.set_keyspace('weather')
            
            self.session.execute("""
                CREATE TYPE IF NOT EXISTS station_record (tmin int, tmax int)
            """)
            print("→ Created UDT")
            
            self.session.execute("""
                CREATE TABLE IF NOT EXISTS stations (
                    id text,
                    date date,
                    name text static,
                    record station_record,
                    PRIMARY KEY (id, date)
                ) WITH CLUSTERING ORDER BY (date ASC)
            """)
            print("→ Created table\n")
            
            # Initialize Spark
            self.spark = SparkSession.builder.appName("p6").getOrCreate()
            print("Spark session ready")
            
            # Load data
            print("Loading station data...")
            self.load_station_data()
            print("Station data loaded")
            
            # Prepare statements
            self.prepare_statements()
            
        except Exception as e:
            print(f"\n!!! INITIALIZATION FAILED !!!\nError: {str(e)}\n")
            raise
    




        # ============ Server Stated Successfully =============
        print("Server started") # Don't delete this line!

    def load_station_data(self):
        # Load and process station data
        df = self.spark.read.text("ghcnd-stations.txt")
        stations = df.select(
            df.value.substr(1, 11).alias("id"),
            df.value.substr(39, 2).alias("state"),
            df.value.substr(41, 30).alias("name")
        ).filter("state = 'WI'").collect()
        
        for row in stations:
            self.session.execute(
                "INSERT INTO weather.stations (id, name) VALUES (%s, %s)",
                (row.id, row.name.strip())
            )
    
    def prepare_statements(self):
        # Prepare statements for later use
        self.insert_record_stmt = self.session.prepare("""
            INSERT INTO weather.stations (id, date, record) 
            VALUES (?, ?, {tmin: ?, tmax: ?})
        """)
        self.insert_record_stmt.consistency_level = ConsistencyLevel.ONE
        
        self.get_name_stmt = self.session.prepare("""
            SELECT name FROM weather.stations WHERE id = ? LIMIT 1
        """)
        
        self.get_max_temp_stmt = self.session.prepare("""
            SELECT MAX(record.tmax) AS max_tmax 
            FROM weather.stations 
            WHERE id = ?
        """)
        self.get_max_temp_stmt.consistency_level = ConsistencyLevel.TWO

    def StationSchema(self, request, context):
        try:
            result = self.session.execute("DESCRIBE TABLE weather.stations")
            schema = ""
            for row in result:
                if "CREATE TABLE" in row.create_statement:
                    schema = row.create_statement
                    break
            return station_pb2.StationSchemaReply(schema=schema, error="")
        except Exception as e:
            return station_pb2.StationSchemaReply(schema="", error=str(e))


    def StationName(self, request, context):
        try:
            result = self.session.execute(self.get_name_stmt, [request.station])
            if result:
                return station_pb2.StationNameReply(name=result[0].name, error="")
            else:
                return station_pb2.StationNameReply(name="", error="Station not found")
        except (cassandra.Unavailable, cassandra.cluster.NoHostAvailable):
            return station_pb2.StationNameReply(name="", error="unavailable")
        except Exception as e:
            return station_pb2.StationNameReply(name="", error=str(e))


    def RecordTemps(self, request, context):
        try:
            self.session.execute(self.insert_record_stmt, [
                request.station, 
                request.date,
                request.tmin,
                request.tmax
            ])
            return station_pb2.RecordTempsReply(error="")
        except (cassandra.Unavailable, cassandra.cluster.NoHostAvailable):
            return station_pb2.RecordTempsReply(error="unavailable")
        except Exception as e:
            return station_pb2.RecordTempsReply(error=str(e))


    def StationMax(self, request, context):
        try:
            self.cluster.refresh_schema_metadata()
            
            live_nodes = [h for h in self.cluster.metadata.all_hosts() if h.is_up]
            
            if len(live_nodes) < 3:
                return station_pb2.StationMaxReply(tmax=-1, error="unavailable")
                
            result = self.session.execute(
                self.get_max_temp_stmt, 
                [request.station],
                timeout=1 
            )
            
            max_temp = result[0].max_tmax if result and result[0].max_tmax else -1
            return station_pb2.StationMaxReply(tmax=max_temp, error="")
            
        except Exception as e:  # Catch ALL exceptions
            return station_pb2.StationMaxReply(tmax=-1, error="unavailable")

def serve():
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=9),
        options=[("grpc.so_reuseport", 0)],
    )
    station_pb2_grpc.add_StationServicer_to_server(StationService(), server)
    server.add_insecure_port('0.0.0.0:5440')
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve()

