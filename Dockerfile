FROM ubuntu:20.04

# Basic stuff
RUN apt update
RUN apt install -y curl build-essential vim

# Unix ODBC
RUN curl ftp://ftp.unixodbc.org/pub/unixODBC/unixODBC-2.3.1.tar.gz -O
RUN tar xf unixODBC-2.3.1.tar.gz
WORKDIR /unixODBC-2.3.1
RUN ./configure --disable-gui --disable-drivers --enable-iconv --with-iconv-char-enc=UTF8 --with-iconv-ucode-enc=UTF16LE --build=aarch64-unknown-linux-gnu
RUN make
RUN make install
RUN echo "/usr/local/lib/" >> /etc/ld.so.conf.d/aarch64-linux-gnu.conf
RUN ldconfig

# FreeTDS (MSSQL ODBC driver)
WORKDIR /
RUN curl -O https://www.freetds.org/files/stable/freetds-0.91.49.tar.bz2 -O
RUN tar xf freetds-0.91.49.tar.bz2
WORKDIR /freetds-0.91.49
RUN ./configure --build=aarch64-unknown-linux-gnu
RUN make
RUN make install
WORKDIR /
COPY build/drivers/mssql.driver.template /
RUN odbcinst -i -d -f mssql.driver.template

# Golang 1.18 Linux x86
RUN curl https://dl.google.com/go/go1.18.linux-amd64.tar.gz -O
RUN rm -rf /usr/local/go && tar -C /usr/local -xzf go1.18.linux-amd64.tar.gz
ENV PATH="/usr/local/go/bin:${PATH}"

EXPOSE 9000

RUN mkdir go && mkdir go/src && mkdir go/src/sqlpipe

COPY go.mod /go/src/sqlpipe
COPY go.sum /go/src/sqlpipe
COPY cmd/ /go/src/sqlpipe/cmd
COPY pkg/ /go/src/sqlpipe/pkg
COPY internal/ /go/src/sqlpipe/internal
COPY vendor/ /go/src/sqlpipe/vendor

WORKDIR /
RUN rm unixODBC-2.3.1.tar.gz && rm freetds-0.91.49.tar.bz2 && rm go1.18.linux-amd64.tar.gz

WORKDIR /go/src/sqlpipe
RUN go build -ldflags=${linker_flags} -o=./bin/sqlpipe ./cmd/sqlpipe
RUN cp bin/sqlpipe /usr/local/bin

WORKDIR /

CMD tail -f /dev/null