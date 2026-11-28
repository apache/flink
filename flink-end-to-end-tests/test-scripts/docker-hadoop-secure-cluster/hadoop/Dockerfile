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

# Hadoop 3.4.3's container-executor links against OpenSSL 1.1 (libcrypto.so.1.1),
# which AlmaLinux 9 ships via compat-openssl11 alongside maintained OpenJDK 17/21 packages.
FROM almalinux:9

ARG JAVA_VERSION=21

RUN set -x \
    && groupadd hadoop \
    && useradd -d /home/hdfs -ms /bin/bash -G hadoop -p hdfs hdfs \
    && useradd -d /home/yarn -ms /bin/bash -G hadoop -p yarn yarn \
    && useradd -d /home/mapred -ms /bin/bash -G hadoop -p mapred mapred \
    && useradd -d /home/hadoop-user -ms /bin/bash -p hadoop-user hadoop-user

# install JDK, OpenSSL 1.1 compat lib, and cluster tools
RUN set -x \
    && dnf install -y --setopt=install_weak_deps=False \
       java-${JAVA_VERSION}-openjdk-devel \
       compat-openssl11 \
       tar sudo openssh-server openssh-clients rsync unzip zip gcc krb5-workstation net-tools procps-ng which \
    && dnf clean all

ENV JAVA_HOME=/usr/lib/jvm/java-${JAVA_VERSION}-openjdk

# Kerberos client
RUN set -x \
    && mkdir -p /var/log/kerberos \
    && touch /var/log/kerberos/kadmind.log

# passwordless ssh
RUN set -x \
    && rm -f /etc/ssh/ssh_host_*_key /etc/ssh/ssh_host_*_key.pub /root/.ssh/id_rsa \
    && ssh-keygen -A \
    && ssh-keygen -q -N "" -t rsa -f /root/.ssh/id_rsa \
    && cp /root/.ssh/id_rsa.pub /root/.ssh/authorized_keys

COPY hadoop.tar.gz /tmp/hadoop.tar.gz

RUN set -x \
    && mkdir -p /usr/local/hadoop \
    && tar -xf /tmp/hadoop.tar.gz --strip-components=1 -C /usr/local/hadoop \
    && rm /tmp/hadoop.tar.gz*

WORKDIR /usr/local
RUN set -x \
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

ENV HADOOP_HOME=/usr/local/hadoop
ENV HADOOP_COMMON_HOME=/usr/local/hadoop
ENV HADOOP_HDFS_HOME=/usr/local/hadoop
ENV HADOOP_MAPRED_HOME=/usr/local/hadoop
ENV HADOOP_YARN_HOME=/usr/local/hadoop
ENV HADOOP_CONF_DIR=/usr/local/hadoop/etc/hadoop
ENV HADOOP_LOG_DIR=/var/log/hadoop
ENV HADOOP_BIN_HOME=$HADOOP_HOME/bin
ENV PATH=$PATH:$HADOOP_BIN_HOME

ENV KRB_REALM=EXAMPLE.COM
ENV DOMAIN_REALM=example.com
ENV KERBEROS_ADMIN_USER=admin/admin
ENV KERBEROS_ADMIN_PASSWORD=admin
ENV KEYTAB_DIR=/etc/security/keytabs

RUN set -x \
    && mkdir /var/log/hadoop \
    && chown root:hadoop /var/log/hadoop \
    && chmod 775 /var/log/hadoop

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
    && mkdir -p /etc/ssh/sshd_config.d \
    && printf 'UsePAM no\nPort 2122\n' > /etc/ssh/sshd_config.d/00-flink.conf

# Hdfs ports
EXPOSE 9000 9866 9867 9864 9868 9865 9869 8020
# Mapred ports
EXPOSE 19888
# Yarn ports
EXPOSE 8030 8031 8032 8033 8040 8042 8088 8188
# Other ports
EXPOSE 49707 2122

ADD entrypoint.sh /entrypoint.sh
RUN chown root:root /entrypoint.sh
RUN chmod 700 /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
CMD ["-h"]
