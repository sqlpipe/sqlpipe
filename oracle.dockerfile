FROM ghcr.io/oracle/oraclelinux8-instantclient:21

# Install UnixODBC
RUN yum update -y
RUN yum install gcc make -y
RUN curl -O ftp://ftp.unixodbc.org/pub/unixODBC/unixODBC-2.3.1.tar.gz
RUN tar xf unixODBC-2.3.1.tar.gz
RUN rm unixODBC-2.3.1.tar.gz /unixODBC-2.3.1/configure
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
RUN mkdir /driver-templates

# psqlodbc
RUN yum install -y postgresql-devel
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
RUN rm -rf freetds-1.3

# Oracle
RUN curl -O https://download.oracle.com/otn_software/linux/instantclient/217000/oracle-instantclient-odbc-21.7.0.0.0-1.el8.x86_64.rpm
RUN rpm -Uvh oracle-instantclient-odbc-21.7.0.0.0-1.el8.x86_64.rpm
COPY build/oracle.driver.template /driver-templates
RUN odbcinst -i -d -f /driver-templates/oracle.driver.template
RUN export ORACLE_HOME=/usr/lib/oracle/21/client64
RUN export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$ORACLE_HOME/lib

# add to data sources ini
COPY build/inis.txt /usr/local/etc/odbc.ini

WORKDIR /
COPY /bin/sqlpipe /

# Run SQLpipe
CMD tail -f /dev/null