FROM python:3.11-slim
RUN pip install --upgrade pip && pip3 install docker

COPY health_checker /
COPY shared /shared
ENTRYPOINT ["/bin/sh"]