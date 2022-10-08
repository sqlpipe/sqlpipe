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

# MSSQL
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

# MySQL
RUN apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 467B942D3A79BD29
RUN echo "deb http://repo.mysql.com/apt/ubuntu/ jammy  mysql-8.0" > /etc/apt/sources.list.d/mysql.list
RUN apt-get update
RUN apt-get install -y libodbc2 libodbcinst2 mysql-community-client-plugins
RUN curl -O http://repo.mysql.com/apt/ubuntu/pool/mysql-tools/m/mysql-connector-odbc/mysql-connector-odbc_8.0.30-1ubuntu22.04_amd64.deb
RUN dpkg -i mysql-connector-odbc_8.0.30-1ubuntu22.04_amd64.deb
RUN rm mysql-connector-odbc_8.0.30-1ubuntu22.04_amd64.deb

COPY build/mysql.driver.template /driver-templates
RUN odbcinst -i -d -f /driver-templates/mysql.driver.template

# Snowflake
RUN curl -O https://sfc-repo.snowflakecomputing.com/odbc/linux/2.25.4/snowflake_linux_x8664_odbc-2.25.4.tgz
RUN gzip -d snowflake_linux_x8664_odbc-2.25.4.tgz
RUN tar -xvf snowflake_linux_x8664_odbc-2.25.4.tar
RUN rm snowflake_linux_x8664_odbc-2.25.4.tar
RUN snowflake_odbc/unixodbc_setup.sh
COPY build/snowflake.driver.template /driver-templates
RUN odbcinst -i -d -f /driver-templates/snowflake.driver.template


# Install SQLpipe
WORKDIR /
COPY /bin/sqlpipe /

# Run SQLpipe
CMD ./sqlpipe