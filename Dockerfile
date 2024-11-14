# First-time build can take upto 10 mins.

FROM apache/airflow:2.8.0

ENV AIRFLOW_HOME=/opt/airflow

USER root
# RUN apt-get update -qq && apt-get install vim -qqq vim mongodb

# Update apt-get and install required dependencies
RUN apt-get update -qq && apt-get install -y wget gnupg

# Add MongoDB GPG key and repository
RUN wget -qO - https://www.mongodb.org/static/pgp/server-6.0.asc | apt-key add - \
    && echo "deb [ arch=amd64,arm64 ] https://repo.mongodb.org/apt/debian bullseye/mongodb-org/6.0 main" | tee /etc/apt/sources.list.d/mongodb-org-6.0.list

# Install MongoDB
RUN apt-get update -qq && apt-get install -y mongodb-org

# Clean up to reduce image size
RUN apt-get clean && rm -rf /var/lib/apt/lists/*

# Install vim as well
RUN apt-get update -qq && apt-get install -y vim



# git gcc g++ -qqq

# Ref: https://airflow.apache.org/docs/docker-stack/recipes.html

SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]

ARG CLOUD_SDK_VERSION=322.0.0
ENV GCLOUD_HOME=/home/google-cloud-sdk

ENV PATH="${GCLOUD_HOME}/bin/:${PATH}"

RUN DOWNLOAD_URL="https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-${CLOUD_SDK_VERSION}-linux-x86_64.tar.gz" \
    && TMP_DIR="$(mktemp -d)" \
    && curl -fL "${DOWNLOAD_URL}" --output "${TMP_DIR}/google-cloud-sdk.tar.gz" \
    && mkdir -p "${GCLOUD_HOME}" \
    && tar xzf "${TMP_DIR}/google-cloud-sdk.tar.gz" -C "${GCLOUD_HOME}" --strip-components=1 \
    && "${GCLOUD_HOME}/install.sh" \
       --bash-completion=false \
       --path-update=false \
       --usage-reporting=false \
       --quiet \
    && rm -rf "${TMP_DIR}" \
    && gcloud --version

# Install polars and pymongo Python packages
RUN pip install pandas pymongo google-cloud-storage
RUN pip install apache-airflow-providers-mongo
RUN pip install pymongo google-cloud-storage google-cloud-bigquery pandas pyarrow


WORKDIR $AIRFLOW_HOME

COPY scripts scripts

RUN chmod +x scripts

USER $AIRFLOW_UID