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

FROM ubuntu:xenial

#requirements
RUN apt-get update && apt-get install -y curl supervisor openssh-server openssh-client \
  openjdk-8-jre-headless \
  && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/ PATH=$PATH:$JAVA_HOME/bin

#In case you want to install Oracle JDK 8 remove the openjdk-8-jre-headless package above and uncomment:
#RUN mkdir -p /usr/java/default && \
#    curl -Ls 'http://download.oracle.com/otn-pub/java/jdk/8u91-b14/jdk-8u91-linux-x64.tar.gz' -H 'Cookie: oraclelicense=accept-securebackup-cookie' | \
#    tar --strip-components=1 -xz -C /usr/java/default/
#ENV JAVA_HOME /usr/java/default/ PATH=$PATH:$JAVA_HOME/bin

#privilege separation directory
RUN mkdir /var/run/sshd
RUN echo 'root:secret' | chpasswd

#SSH as root... probably needs to be revised for security!
RUN sed -i 's/PermitRootLogin without-password/PermitRootLogin yes/' /etc/ssh/sshd_config

EXPOSE 22

# supervisor base configuration
ADD supervisor.conf /etc/supervisor/

# default command / not overridable CMD needed for supervisord
#CMD ["supervisord", "-c", "/etc/supervisor.conf"]
