import grpc
from concurrent import futures
import table_pb2_grpc, table_pb2
import pyarrow as pa
import pyarrow.csv
import pyarrow.parquet
import threading
import os
file_paths=[]
lock=threading.Lock()
file_counter=0

class TableServicer(table_pb2_grpc.TableServicer):
    def Upload(self, request, context):
        global file_paths, file_counter
        
        with lock:
            file_counter+=1
            csv_filename = f"uploads/file_{file_counter}.csv"
            parquet_filename = f"uploads/file_{file_counter}.parquet"
        os.makedirs("uploads", exist_ok=True)
        with open(csv_filename, "wb") as f:
            f.write(request.csv_data)

        table = pyarrow.csv.read_csv(csv_filename)
        pyarrow.parquet.write_table(table, parquet_filename)

        with lock:
            file_paths.append((csv_filename, parquet_filename))
        return table_pb2.UploadResp(error="")

    def ColSum(self, request, context):
        column = request.column
        format = request.format
        total = 0

        with lock:
            for csv_path, parquet_path in file_paths:
                try:
                    
                    if format == "csv":
                        table = pyarrow.csv.read_csv(csv_path)
                        print("before column")
                        if column in table.column_names:
                            print("Inside column")
                            column_data = table.column(column)
                            total += sum(column_data.to_pylist())
                    
                    elif format == "parquet":
                        table = pyarrow.parquet.read_table(parquet_path)
                        print("Outside parquet cloumn")
                        if column in table.column_names:
                            print("Inside parquet cloumn")
                            column_data = table.column(column)
                            total += sum(column_data.to_pylist())
                
                except Exception as e:
                    return table_pb2.ColSumResp(error=str(e))
                
        return table_pb2.ColSumResp(total=total, error="")

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8),
                         options=[("grpc.so_reuseport", 0)])
    table_pb2_grpc.add_TableServicer_to_server(TableServicer(), server)
    server.add_insecure_port("localhost:5440")
    print("Server started on localhost:5440")
    server.start()
    server.wait_for_termination()

if __name__ == "__main__":
    serve()