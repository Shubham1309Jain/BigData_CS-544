import grpc
import station_pb2
import station_pb2_grpc
import sys

# As both the client and server are running on container p6-db-1, we can use localhost as the server address
SERVER = "localhost:5440"

def run():
    if len(sys.argv) != 2:
        print("Usage: python3 ClientStationMax.py <StationID>")
        sys.exit(1)
    stationID = sys.argv[1]

    # Connect to the gRPC server
    with grpc.insecure_channel(SERVER) as channel:
        stub = station_pb2_grpc.StationStub(channel)
        # Send the request and get the number of stations
        response = stub.StationMax(station_pb2.StationInspectRequest(station = stationID))
        if response.error != "":
            print(response.error)
        else:
            print(response.tmax)

if __name__ == '__main__':
    run()