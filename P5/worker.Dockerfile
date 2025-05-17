FROM p5-base
CMD ["bash", "-c", "/spark-3.5.5-bin-hadoop3/sbin/start-worker.sh spark://boss:7077 && tail -f /dev/null"]