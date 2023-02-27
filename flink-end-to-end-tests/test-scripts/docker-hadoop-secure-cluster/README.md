# Apache Hadoop Docker image with Kerberos enabled

Required versions
-----------------

* JDK8
* Hadoop 2.10.2

Default Environment Variables
-----------------------------

| Name                      | Value | Description                                                                                                                                     |
|---------------------------| ----  |-------------------------------------------------------------------------------------------------------------------------------------------------|
| `KRB_REALM`               | `EXAMPLE.COM` | The Kerberos Realm, more information [here](https://web.mit.edu/kerberos/krb5-1.12/doc/admin/conf_files/krb5_conf.html#)                        |
| `DOMAIN_REALM`            | `example.com` | The Kerberos Domain Realm, more information [here](https://web.mit.edu/kerberos/krb5-1.12/doc/admin/conf_files/krb5_conf.html#)                 |
| `KERB_MASTER_KEY`         | `masterkey` | The Kerberos master database password, more information [here](https://web.mit.edu/kerberos/krb5-1.12/doc/admin/admin_commands/kdb5_util.html#) |
| `KERBEROS_ADMIN_USER`     | `admin/admin` | The KDC admin user                                                                                                                              |
| `KERBEROS_ADMIN_PASSWORD` | `admin` | The KDC admin password                                                                                                                          |

You can simply define these variables in the `docker-compose.yml`.

Run image
---------

```
cd flink-end-to-end-tests/test-scripts/docker-hadoop-secure-cluster
wget -O hadoop/hadoop.tar.gz https://archive.apache.org/dist/hadoop/common/hadoop-2.10.2/hadoop-2.10.2.tar.gz
docker-compose build
docker-compose up
```

Usage
-----

Get the container name with `docker ps` and login to the container with

```
docker exec -it <container-name> /bin/bash
```

To obtain a Kerberos ticket, execute

```
kinit -kt /home/hadoop-user/hadoop-user.keytab hadoop-user
```

Afterwards you can use `hdfs` CLI like

```
hdfs dfs -ls /
```

Known issues
------------

### Unable to obtain Kerberos password

#### Error
docker-compose up fails for the first time with the error

```
Login failure for nn/hadoop.docker.com@EXAMPLE.COM from keytab /etc/security/keytabs/nn.service.keytab: javax.security.auth.login.LoginException: Unable to obtain password from user
```

#### Solution

Stop the containers with `docker-compose down` and start again with `docker-compose up -d`.

### Java Keystore

If the Keystore has been expired, then create a new `keystore.jks`:

1. create private key

```
openssl genrsa -des3 -out server.key 1024
```

2. create csr

```
openssl req -new -key server.key -out server.csr`
```

3. remove passphrase in key
```
cp server.key server.key.org
openssl rsa -in server.key.org -out server.key
```

3. create self-signed cert
```
openssl x509 -req -days 365 -in server.csr -signkey server.key -out server.crt
```

4. create JKS and import certificate
```
keytool -import -keystore keystore.jks -alias CARoot -file server.crt`
```
