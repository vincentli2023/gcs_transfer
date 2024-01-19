FROM ubuntu:22.04
# ENV DEBIAN_FRONTEND=noninteractive
RUN mkdir /root/gcs_transfer

COPY ./requirements.txt /tmp/requirements.txt
RUN apt-get update && apt-get install -y iputils-ping python3-pip && apt-get clean && pip3 --no-cache-dir install --upgrade pip
RUN apt-get install -y apt-transport-https ca-certificates gnupg curl

RUN pip3 install -r /tmp/requirements.txt && rm /tmp/requirements.txt

# Add the Google Cloud SDK distribution URI as a package source
RUN curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg
RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list
RUN apt-get update && apt-get install google-cloud-cli

COPY ./ /root/gcs_transfer/
ENV PYTHONPATH "${PYTHONPATH}:/root/gcs_transfer/"

# FOR GCP ACCESS
ENV GOOGLE_APPLICATION_CREDENTIALS=/root/gcs_transfer/gcp_key.json
RUN gcloud auth activate-service-account --key-file=$GOOGLE_APPLICATION_CREDENTIALS

WORKDIR /root