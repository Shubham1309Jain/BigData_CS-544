import grpc
import PropertyLookup_pb2
import PropertyLookup_pb2_grpc

channel = grpc.insecure_channel("localhost:5000")
stub = PropertyLookup_pb2_grpc.PropertyLookupStub(channel)

response = stub.LookupByZip(PropertyLookup_pb2.LookupRequest(zip=53716, limit=10))

print("Response received: ", response) 
