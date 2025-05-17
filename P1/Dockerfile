# Use a base image
FROM ubuntu:24.04

# Install dependencies
RUN apt-get update && apt-get install -y \
    wget \
    gzip \
    grep

# Copy scripts into the container
COPY download.sh /usr/local/bin/download.sh
COPY multi.sh /usr/local/bin/multi.sh

# Make scripts executable
RUN chmod +x /usr/local/bin/download.sh
RUN chmod +x /usr/local/bin/multi.sh

# Set working directory
WORKDIR /data

# Run the multi.sh script
CMD ["/usr/local/bin/multi.sh"]
