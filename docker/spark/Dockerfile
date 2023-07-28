FROM bitnami/spark:3.4
USER root

COPY ./jars/* /opt/bitnami/spark/jars

# Install additional libraries
COPY requirements.txt .
RUN pip install -r requirements.txt

# # Install OpenJDK-11
# RUN apt update && \
#     apt-get install -y openjdk-11-jdk && \
#     apt-get install -y ant && \
#     apt-get clean;

# # Set JAVA_HOME
# ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
# RUN export JAVA_HOME

# USER airflow

# COPY ./requirements.txt /
# RUN pip install -r /requirements.txt

# COPY --chown=airflow:root dags /opt/airflow/dags