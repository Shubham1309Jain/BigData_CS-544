FROM ubuntu:22.04

# Install required packages
RUN apt-get update && apt-get install -y \
    wget \
    curl \
    openjdk-11-jdk \
    python3-pip \
    net-tools \
    lsof \
    nano \
    sudo


# Install Python dependencies
RUN pip3 install jupyterlab==4.3.5 pandas==2.2.3 pyspark==3.5.5 matplotlib==3.10.1

# Download and extract Hadoop
RUN wget https://dlcdn.apache.org/hadoop/common/hadoop-3.4.1/hadoop-3.4.1.tar.gz && \
    tar -xf hadoop-3.4.1.tar.gz && \
    rm hadoop-3.4.1.tar.gz

# Download and extract Spark
RUN wget https://dlcdn.apache.org/spark/spark-3.5.5/spark-3.5.5-bin-hadoop3.tgz && \
    tar -xf spark-3.5.5-bin-hadoop3.tgz && \
    rm spark-3.5.5-bin-hadoop3.tgz


# Set environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH="${PATH}:/hadoop-3.4.1/bin"
ENV HADOOP_HOME=/hadoop-3.4.1
