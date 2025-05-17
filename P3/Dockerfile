FROM python:3.9-slim

WORKDIR /app

COPY . .

RUN python3 -m pip install grpcio-tools==1.70.0 grpcio==1.70.0 protobuf==5.29.3 pyarrow pandas

RUN python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. table.proto

CMD ["python3", "server.py"]