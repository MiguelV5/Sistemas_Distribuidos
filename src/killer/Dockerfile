FROM python:3.11-slim
RUN pip install --upgrade pip && pip3 install docker

COPY src/killer /
COPY src/shared /shared
ENTRYPOINT ["/bin/sh"]