import grpc
from concurrent import futures
import lender_pb2
import lender_pb2_grpc
import pandas as pd
import mysql.connector
import pyarrow.parquet as pq
import pyarrow as pa
import pyarrow.fs as fs
import requests


class LenderServicer(lender_pb2_grpc.LenderServicer):
    def __init__(self):
        # Set HDFS configuration programmatically
        hdfs_config = {
            "fs.defaultFS": "hdfs://boss:9000",  # HDFS NameNode URL
            "dfs.replication": "2"              # Replication factor
        }
        # Initialize HadoopFileSystem with the configuration
        self.hdfs = fs.HadoopFileSystem('boss', port=9000, extra_conf=hdfs_config)

    def DbToHdfs(self, request, context):
        try:
            # Connect to MySQL
            conn = mysql.connector.connect(
                host="mysql",
                user="root",
                password="abc",
                database="CS544"
            )

            # Perform the join and filter using pandas
            query = """
                SELECT l.*, lt.loan_type_name
                FROM loans l
                JOIN loan_types lt ON l.loan_type_id = lt.id
                WHERE l.loan_amount > 30000 AND l.loan_amount < 800000
            """
            df = pd.read_sql(query, conn)

            # Write to Parquet and upload to HDFS
            table = pa.Table.from_pandas(df)
            with self.hdfs.open_output_stream("/hdma-wi-2021.parquet") as f:
                pq.write_table(table, f)

            # Move the file to the root directory in HDFS (if needed)
            # os.system("hdfs dfs -fs hdfs://boss:9000 -mv /hdma-wi-2021.parquet /")

            return lender_pb2.StatusString(status="Data uploaded to HDFS successfully")

        except Exception as e:
            return lender_pb2.StatusString(status=f"Error: {str(e)}")

    def BlockLocations(self, request, context):
        try:
            # Use WebHDFS API to get block locations
            url = f"http://boss:9870/webhdfs/v1{request.path}?op=GETFILEBLOCKLOCATIONS"
            print("WebHDFS URL:", url)
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            print("WebHDFS Response:", data)

            # Extract block locations
            block_entries = {}
            # Adjust the key access based on the actual JSON structure
            blocks = data.get('BlockLocations', {}).get('BlockLocation', [])
            print("Blocks:", blocks)
            for block in blocks:
                hosts = block.get('hosts', [])
                print("Hosts for Block:", hosts)
                for host in hosts:
                    # datanode = location.get('host', '')
                    if host:
                        block_entries[host] = block_entries.get(host, 0) + 1
            print("Block Entries:", block_entries)
            return lender_pb2.BlockLocationsResp(block_entries=block_entries)

        except Exception as e:
            print("Error:", e)
            return lender_pb2.BlockLocationsResp(error=str(e))
        
    def CalcAvgLoan(self, request, context):
        try:
            county_code = request.county_code
            file_path = f"/partitions/{county_code}.parquet"

            # Check if county-specific file exists
            try:
                with self.hdfs.open_input_stream(file_path) as f:
                    table = pq.read_table(f)
                source = "reuse"
            except FileNotFoundError:
                # Read from the big file and filter
                with self.hdfs.open_input_stream("/hdma-wi-2021.parquet") as f:
                    table = pq.read_table(f)
                df = table.to_pandas()
                df = df[df['county_code'] == county_code]

                # Write to county-specific file
                table = pa.Table.from_pandas(df)
                with self.hdfs.open_output_stream(file_path) as f:
                    pq.write_table(table, f)
                source = "create"

            # Calculate average loan amount
            avg_loan = int(df['loan_amount'].mean())

            return lender_pb2.CalcAvgLoanResp(avg_loan=avg_loan, source=source)

        except Exception as e:
            return lender_pb2.CalcAvgLoanResp(error=str(e))

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    lender_pb2_grpc.add_LenderServicer_to_server(LenderServicer(), server)
    server.add_insecure_port('[::]:5000')
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve()