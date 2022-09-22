FROM golang:1.19.1-bullseye

WORKDIR /

RUN apt-get update
RUN apt-get remove libodbc1 unixodbc unixodbc-dev
RUN apt-get install -y curl
RUN apt-get install -y vim
RUN apt-get install -y libpq-dev
RUN curl -O ftp://ftp.unixodbc.org/pub/unixODBC/unixODBC-2.3.1.tar.gz
RUN tar xf unixODBC-2.3.1.tar.gz
RUN rm unixODBC-2.3.1.tar.gz
RUN rm /unixODBC-2.3.1/configure
COPY build/configure /unixODBC-2.3.1
RUN apt-get -y install build-essential
WORKDIR /unixODBC-2.3.1
RUN ./configure --disable-gui --disable-drivers --enable-iconv --with-iconv-char-enc=UTF8 --with-iconv-ucode-enc=UTF16LE --build=aarch64-unknown-linux-gnu
RUN make
RUN make install
RUN echo "/usr/local/lib/" >> /etc/ld.so.conf.d/x86_64-linux-gnu.conf
RUN ldconfig
WORKDIR /
RUN curl -O https://www.freetds.org/files/stable/freetds-1.3.tar.gz
RUN tar xf freetds-1.3.tar.gz
RUN rm freetds-1.3.tar.gz
WORKDIR /freetds-1.3
RUN ./configure
RUN make
RUN make install
WORKDIR /
RUN mkdir driver-templates
COPY build/mssql.driver.template /driver-templates
RUN odbcinst -i -d -f /driver-templates/mssql.driver.template
RUN curl -O https://ftp.postgresql.org/pub/odbc/versions/src/psqlodbc-13.02.0000.tar.gz
RUN tar -xf psqlodbc-13.02.0000.tar.gz
RUN rm psqlodbc-13.02.0000.tar.gz
WORKDIR /psqlodbc-13.02.0000
RUN ./configure --with-unixodbc
RUN make
RUN make install
COPY build/postgresql.driver.template /driver-templates
RUN odbcinst -i -d -f /driver-templates/postgresql.driver.template
RUN mkdir /go/src/sqlpipe
COPY cmd /go/src/sqlpipe/cmd
COPY internal /go/src/sqlpipe/internal
COPY pkg /go/src/sqlpipe/pkg
COPY vendor /go/src/sqlpipe/vendor
COPY go.mod /go/src/sqlpipe
COPY go.sum /go/src/sqlpipe
WORKDIR /go/src/sqlpipe
RUN go build -ldflags="-s" -o=/sqlpipe ./cmd/sqlpipe

WORKDIR /

ARG SECURE="--secure=false"
ENV SECURE="${SECURE}"

CMD ./sqlpipe ${SECURE}

# CMD bash