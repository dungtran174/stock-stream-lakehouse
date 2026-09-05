FROM apache/airflow:2.8.0-python3.9

USER root

# Install Java OpenJDK and utilities required for SparkSubmitOperator
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    default-jdk \
    curl \
    && apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set Java & Spark environment variables
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

# Download and install Spark 3.5.0 client binaries
RUN curl -fSL "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" -o /tmp/spark.tgz && \
    mkdir -p ${SPARK_HOME} && \
    tar -xzf /tmp/spark.tgz -C ${SPARK_HOME} --strip-components=1 && \
    rm /tmp/spark.tgz

USER airflow

# Install custom python packages and airflow providers
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
