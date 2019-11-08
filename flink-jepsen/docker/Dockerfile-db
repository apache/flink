################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

FROM debian:stretch

RUN apt-get update && \
    apt-get install -qqy \
            apt-utils \
            bzip2 \
            curl \
            faketime \
            gnupg \
            iproute \
            iptables \
            iputils-ping \
            less \
            libzip4 \
            logrotate \
            man \
            man-db \
            net-tools \
            ntpdate \
            openjdk-8-jdk \
            psmisc python \
            rsyslog \
            runit \
            sudo \
            tar \
            unzip \
            vim \
            wget

RUN apt-get update && \
    apt-get -y install openssh-server && \
    mkdir -p /var/run/sshd && \
    sed -i "s/UsePrivilegeSeparation.*/UsePrivilegeSeparation no/g" /etc/ssh/sshd_config && \
    sed -i "s/PermitRootLogin without-password/PermitRootLogin yes/g" /etc/ssh/sshd_config

ADD id_rsa.pub /root
RUN mkdir -p /root/.ssh/ && \
    touch /root/.ssh/authorized_keys && \
    chmod 600 /root/.ssh/authorized_keys && \
    cat /root/id_rsa.pub >> /root/.ssh/authorized_keys

COPY sshd-run /etc/sv/service/sshd/run
RUN chmod +x /etc/sv/service/sshd/run && \
    ln -sf /etc/sv/service/sshd /etc/service

EXPOSE 22

# Start runit process supervisor which will bring up sshd.
# In our tests we can use runit to supervise more processes, e.g., Mesos.
CMD runsvdir -P /etc/service /dev/null > /dev/null
