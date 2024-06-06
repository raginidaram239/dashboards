FROM europe-west1-docker.pkg.dev/databutton/public/databutton-base:REPLACE_ME_VERSION

WORKDIR /app

ARG DATABUTTON_TOKEN

ENV PYTHONBUFFERED=1

ENV DATABUTTON_TOKEN=$DATABUTTON_TOKEN

COPY requirements.txt ./requirements.txt
RUN pip install -r requirements.txt

COPY . /app

ENTRYPOINT ["databutton"]
