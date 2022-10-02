FROM ubuntu:22.04

# Install ODBC
WORKDIR /
RUN apt-get update
RUN apt-get install -y vim curl libpq-dev build-essential
RUN curl -O ftp://ftp.unixodbc.org/pub/unixODBC/unixODBC-2.3.1.tar.gz
RUN tar xf unixODBC-2.3.1.tar.gz
RUN rm unixODBC-2.3.1.tar.gz
RUN rm /unixODBC-2.3.1/configure
COPY build/configure /unixODBC-2.3.1
WORKDIR /unixODBC-2.3.1
RUN ./configure --disable-gui --disable-drivers --enable-iconv --with-iconv-char-enc=UTF8 --with-iconv-ucode-enc=UTF16LE
RUN make
RUN make install
WORKDIR /
RUN rm -rf /unixODBC-2.3.1
RUN echo "/usr/local/lib/" >> /etc/ld.so.conf.d/x86_64-linux-gnu.conf
RUN ldconfig

# Install ODBC drivers
WORKDIR /
RUN mkdir driver-templates

# PostgreSQL, psqlodbc
WORKDIR /
RUN curl -O https://ftp.postgresql.org/pub/odbc/versions/src/psqlodbc-13.02.0000.tar.gz
RUN tar -xf psqlodbc-13.02.0000.tar.gz
RUN rm psqlodbc-13.02.0000.tar.gz
WORKDIR /psqlodbc-13.02.0000
RUN ./configure --with-unixodbc
RUN make
RUN make install
COPY build/postgresql.driver.template /driver-templates
RUN odbcinst -i -d -f /driver-templates/postgresql.driver.template
WORKDIR /
RUN rm -rf psqlodbc-13.02.0000

# MSSQL, freetds
WORKDIR /
RUN curl -O https://www.freetds.org/files/stable/freetds-1.3.tar.gz
RUN tar xf freetds-1.3.tar.gz
RUN rm freetds-1.3.tar.gz
WORKDIR /freetds-1.3
RUN ./configure
RUN make
RUN make install
WORKDIR /
COPY build/mssql.driver.template /driver-templates
RUN odbcinst -i -d -f /driver-templates/mssql.driver.template
WORKDIR /
RUN rm -rf freetds-1.3

# Install SQLpipe
WORKDIR /
COPY /bin/sqlpipe /

# Run SQLpipe
WORKDIR /
CMD ./sqlpipe