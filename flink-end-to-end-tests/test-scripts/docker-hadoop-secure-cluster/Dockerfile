################################################################################
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
#
# This image is modified version of Knappek/docker-hadoop-secure
#   * Knappek/docker-hadoop-secure <https://github.com/Knappek/docker-hadoop-secure>
#
# With bits and pieces added from Lewuathe/docker-hadoop-cluster to extend it to start a proper kerberized Hadoop cluster:
#   * Lewuathe/docker-hadoop-cluster <https://github.com/Lewuathe/docker-hadoop-cluster>
#
# Creates multi-node, kerberized Hadoop cluster on Docker

FROM sequenceiq/pam:ubuntu-14.04

RUN set -x \
    && addgroup hadoop \
    && useradd -d /home/hdfs -ms /bin/bash -G hadoop -p hdfs hdfs \
    && useradd -d /home/yarn -ms /bin/bash -G hadoop -p yarn yarn \
    && useradd -d /home/mapred -ms /bin/bash -G hadoop -p mapred mapred \
    && useradd -d /home/hadoop-user -ms /bin/bash -p hadoop-user hadoop-user

# install dev tools
RUN set -x \
    && apt-get update && apt-get install -y \
    curl tar sudo openssh-server openssh-client rsync unzip krb5-user

# Kerberos client
RUN set -x \
    && mkdir -p /var/log/kerberos \
    && touch /var/log/kerberos/kadmind.log

# passwordless ssh
RUN set -x \
    && rm -f /etc/ssh/ssh_host_dsa_key /etc/ssh/ssh_host_rsa_key /root/.ssh/id_rsa \
    && ssh-keygen -q -N "" -t dsa -f /etc/ssh/ssh_host_dsa_key \
    && ssh-keygen -q -N "" -t rsa -f /etc/ssh/ssh_host_rsa_key \
    && ssh-keygen -q -N "" -t rsa -f /root/.ssh/id_rsa \
    && cp /root/.ssh/id_rsa.pub /root/.ssh/authorized_keys

# java
RUN set -x \
    && mkdir -p /usr/java/default \
    && curl -Ls 'http://download.oracle.com/otn-pub/java/jdk/8u131-b11/d54c1d3a095b4ff2b6607d096fa80163/jdk-8u131-linux-x64.tar.gz' -H 'Cookie: oraclelicense=accept-securebackup-cookie' | \
        tar --strip-components=1 -xz -C /usr/java/default/

ENV JAVA_HOME /usr/java/default
ENV PATH $PATH:$JAVA_HOME/bin

RUN set -x \
    && curl -LOH 'Cookie: oraclelicense=accept-securebackup-cookie' 'http://download.oracle.com/otn-pub/java/jce/8/jce_policy-8.zip' \
    && unzip jce_policy-8.zip \
    && cp /UnlimitedJCEPolicyJDK8/local_policy.jar /UnlimitedJCEPolicyJDK8/US_export_policy.jar $JAVA_HOME/jre/lib/security

ARG HADOOP_VERSION=2.8.4

ENV HADOOP_URL http://archive.apache.org/dist/hadoop/common/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz
RUN set -x \
    && curl -fSL "$HADOOP_URL" -o /tmp/hadoop.tar.gz \
    && tar -xf /tmp/hadoop.tar.gz -C /usr/local/ \
    && rm /tmp/hadoop.tar.gz*

WORKDIR /usr/local
RUN set -x \
    && ln -s /usr/local/hadoop-${HADOOP_VERSION} /usr/local/hadoop \
    && chown root:root -R /usr/local/hadoop-${HADOOP_VERSION}/ \
    && chown root:root -R /usr/local/hadoop/ \
    && chown root:yarn /usr/local/hadoop/bin/container-executor \
    && chmod 6050 /usr/local/hadoop/bin/container-executor \
    && mkdir -p /hadoop-data/nm-local-dirs \
    && mkdir -p /hadoop-data/nm-log-dirs \
    && chown yarn:yarn /hadoop-data \
    && chown yarn:yarn /hadoop-data/nm-local-dirs \
    && chown yarn:yarn /hadoop-data/nm-log-dirs \
    && chmod 755 /hadoop-data \
    && chmod 755 /hadoop-data/nm-local-dirs \
    && chmod 755 /hadoop-data/nm-log-dirs

ENV HADOOP_HOME /usr/local/hadoop
ENV HADOOP_COMMON_HOME /usr/local/hadoop
ENV HADOOP_HDFS_HOME /usr/local/hadoop
ENV HADOOP_MAPRED_HOME /usr/local/hadoop
ENV HADOOP_YARN_HOME /usr/local/hadoop
ENV HADOOP_CONF_DIR /usr/local/hadoop/etc/hadoop
ENV YARN_CONF_DIR /usr/local/hadoop/etc/hadoop
ENV HADOOP_LOG_DIR /var/log/hadoop
ENV HADOOP_BIN_HOME $HADOOP_HOME/bin
ENV PATH $PATH:$HADOOP_BIN_HOME

ENV KRB_REALM EXAMPLE.COM
ENV DOMAIN_REALM example.com
ENV KERBEROS_ADMIN admin/admin
ENV KERBEROS_ADMIN_PASSWORD admin
ENV KEYTAB_DIR /etc/security/keytabs

RUN mkdir /var/log/hadoop

ADD config/core-site.xml $HADOOP_HOME/etc/hadoop/core-site.xml
ADD config/hdfs-site.xml $HADOOP_HOME/etc/hadoop/hdfs-site.xml
ADD config/mapred-site.xml $HADOOP_HOME/etc/hadoop/mapred-site.xml
ADD config/yarn-site.xml $HADOOP_HOME/etc/hadoop/yarn-site.xml
ADD config/container-executor.cfg $HADOOP_HOME/etc/hadoop/container-executor.cfg
ADD config/krb5.conf /etc/krb5.conf
ADD config/ssl-server.xml $HADOOP_HOME/etc/hadoop/ssl-server.xml
ADD config/ssl-client.xml $HADOOP_HOME/etc/hadoop/ssl-client.xml
ADD config/keystore.jks $HADOOP_HOME/lib/keystore.jks

RUN set -x \
    && chmod 400 $HADOOP_HOME/etc/hadoop/container-executor.cfg \
    && chown root:yarn $HADOOP_HOME/etc/hadoop/container-executor.cfg

ADD config/ssh_config /root/.ssh/config
RUN set -x \
    && chmod 600 /root/.ssh/config \
    && chown root:root /root/.ssh/config

# workingaround docker.io build error
RUN set -x \
    && ls -la /usr/local/hadoop/etc/hadoop/*-env.sh \
    && chmod +x /usr/local/hadoop/etc/hadoop/*-env.sh \
    && ls -la /usr/local/hadoop/etc/hadoop/*-env.sh

# fix the 254 error code
RUN set -x \
    && sed  -i "/^[^#]*UsePAM/ s/.*/#&/"  /etc/ssh/sshd_config \
    && echo "UsePAM no" >> /etc/ssh/sshd_config \
    && echo "Port 2122" >> /etc/ssh/sshd_config

# Hdfs ports
EXPOSE 50470 9000 50010 50020 50070 50075 50090 50475 50091 8020
# Mapred ports
EXPOSE 19888
# Yarn ports
EXPOSE 8030 8031 8032 8033 8040 8042 8088 8188
# Other ports
EXPOSE 49707 2122

ADD bootstrap.sh /etc/bootstrap.sh
RUN chown root:root /etc/bootstrap.sh
RUN chmod 700 /etc/bootstrap.sh

ENV BOOTSTRAP /etc/bootstrap.sh

ENTRYPOINT ["/etc/bootstrap.sh"]
CMD ["-h"]
