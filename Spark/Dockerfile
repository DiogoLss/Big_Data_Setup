FROM bitnami/spark:latest

WORKDIR /opt/bitnami/spark/

COPY jars/*.jar /opt/bitnami/spark/jars/
COPY spark-defaults.conf /opt/bitnami/spark/conf/spark-defaults.conf

EXPOSE 8888 7077

#RODAR NO CMD DO CONTAINER
# mkdir -p /tmp/spark-events
# chmod 777 /tmp/spark-events