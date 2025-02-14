FROM bitnami/spark:latest

WORKDIR /opt/bitnami/spark/

COPY conf/jars/*.jar /opt/bitnami/spark/jars/

EXPOSE 8888 7077

#RODAR NO CMD DO CONTAINER
# mkdir -p /tmp/spark-events
# chmod 777 /tmp/spark-events