FROM debian:12.1

RUN apt-get update
RUN apt-get install -y curl postgresql-client libaio1 unixodbc unixodbc-dev unzip

WORKDIR /

COPY ./bin/tests /usr/local/bin/tests
COPY ./LICENSE.MD /SQLPIPE-LICENSE.MD
CMD ["tail", "-f", "/dev/null"]