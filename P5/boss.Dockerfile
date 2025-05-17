FROM p5-base
CMD ["bash", "-c", "/spark-3.5.5-bin-hadoop3/sbin/start-master.sh -h boss && tail -f /dev/null"]