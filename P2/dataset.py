import grpc
import gzip
import csv
import PropertyLookup_pb2
import PropertyLookup_pb2_grpc
from concurrent import futures
#import requests 

#url = "https://git.doit.wisc.edu/cdis/cs/courses/cs544/s25/main/-/raw/main/p2/addresses.csv.gz"
#response = requests.get(url)

#if response.status_code == 200:
   # with open("addresses.csv.gz", "wb") as f:
       # f.write(response.content)
    #print("File downloaded successfully!")
#else:
    #print("Failed to download file:", response.status_code)
    #exit(1) 

class PropertyLookup(PropertyLookup_pb2_grpc.PropertyLookupServicer):
	def LookupByZip(self, request, context):
		#print("In PropertyLookup!!!")
		addresses = self._load_addresses()
		zipcode = str(request.zip)
		limit = request.limit
		filtered = [address for zip_code, address in addresses if zip_code == zipcode]
		sorted_addresses = sorted(filtered, key=lambda x: x)
		return PropertyLookup_pb2.LookupResponse(addresses=sorted_addresses[:limit])

	def _load_addresses(self):
		with gzip.open("addresses.csv.gz", "rt") as f: # Read addresses from the CSV file	
			reader = csv.DictReader(f)
			return [(row["ZipCode"], row["Address"]) for row in reader]
print("start server")
server = grpc.server(futures.ThreadPoolExecutor(max_workers=1), options=[("grpc.so_reuseport", 0)])
PropertyLookup_pb2_grpc.add_PropertyLookupServicer_to_server(PropertyLookup(), server)
server.add_insecure_port("0.0.0.0:5000")
server.start()
server.wait_for_termination()

