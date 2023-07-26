# Dockerfile for Python 3.10 environment
ARG OWNER=jupyter
ARG BASE_CONTAINER=$OWNER/pyspark-notebook:latest
FROM $BASE_CONTAINER

# Install additional libraries
COPY requirements.txt .
RUN pip install -r requirements.txt

# Expose the Jupyter Lab port
EXPOSE 8888

# Set the working directory
# WORKDIR /notebooks
