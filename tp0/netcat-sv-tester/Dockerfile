FROM alpine:latest

RUN apk update && apk add netcat-openbsd
COPY /netcat-sv-tester /
RUN chmod u+x sv-test.sh
ENTRYPOINT [ "/bin/sh" ]
