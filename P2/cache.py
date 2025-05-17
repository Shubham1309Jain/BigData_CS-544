import flask
from flask import Flask
import grpc
from PropertyLookup_pb2 import LookupRequest
from PropertyLookup_pb2_grpc import PropertyLookupStub
import os
from functools import lru_cache
import time

app = Flask("p2")
project= os.environ.get("PROJECT","p2")
dataset_servers =[f"{project}-dataset-1:5000", f"{project}-dataset-2:5000"] #Two servers are used to balance the load
current_server=0

cache_size = 3
cache={} #key=zipcode, value=Address
evict_order = []
# hits=[]

def get_address(zipcode, limit):
    try:
        if zipcode in cache:
            evict_order.remove(zipcode)
            evict_order.append(zipcode)
            # hits.append(1)
            return cache[zipcode], True
        else:
            channel = grpc.insecure_channel(dataset_servers[current_server])
            stub = PropertyLookupStub(channel)
            response = stub.LookupByZip(LookupRequest(zip=zipcode, limit=limit))
            addresses=response.addresses
            # cache[zipcode]=df
            # evict_order.append(zipcode)
            
            if len(addresses)>=8:
                # cache[zipcode] = addresses[:8]
                # evict_order.append(zipcode)
                if len(cache)>=cache_size:
                    victim=evict_order.pop(0)
                    cache.pop(victim)
                cache[zipcode] = addresses[:8]
                evict_order.append(zipcode)
            # hits.append(0)
        # return response.addresses
        return addresses, False
    except grpc.RpcError:
            return None

@app.route("/lookup/<zipcode>")
def lookup(zipcode):
        global current_server
        zipcode = int(zipcode)
        limit = flask.request.args.get("limit", default=4, type=int)

        try:
            cached_address, is_cache_hit=get_address(zipcode, limit)
            if cached_address and limit<=8:
                    return flask.jsonify({"addrs": cached_address[:limit], "source": "cache" if is_cache_hit else str(current_server + 1) , "error": None})
        except Exception as eg:
             False
        e= None
       
        for i in range (5):
            try:
                print("Trying " + str(current_server) + " ...")
                channel = grpc.insecure_channel(dataset_servers[current_server])
                stub = PropertyLookupStub(channel)
                response = stub.LookupByZip(LookupRequest(zip=zipcode, limit=max(limit, 8)))
                addresses = response.addresses

                if len(addresses) >= 8:
                    cache[zipcode] = addresses[:8]
                    evict_order.append(zipcode)

                    if len(cache) > cache_size:
                        victim = evict_order.pop(0)
                        cache.pop(victim)

                old = current_server        
                current_server=(current_server+1)%2
                return flask.jsonify({"addrs": response.addresses[:limit], "source": str(old + 1), "error": None})

            except grpc.RpcError as err:
                e = err
                current_server=(current_server+1)%2
                time.sleep(0.1)

        return flask.jsonify({"addrs": [], "source": "none", "error": str(e) if e else "Unknown error" })
def main():
        app.run("0.0.0.0", port=8080, debug=True, threaded=False)

if __name__ == "__main__":
        main()

