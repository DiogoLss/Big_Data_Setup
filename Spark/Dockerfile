FROM bitnami/spark:3.5.4

WORKDIR /opt/bitnami/spark/

COPY jars/*.jar /opt/bitnami/spark/jars/
COPY spark-defaults.conf /opt/bitnami/spark/conf/spark-defaults.conf

EXPOSE 4040 7077