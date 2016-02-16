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

FROM base

#add passless key to ssh
RUN ssh-keygen -f ~/.ssh/id_rsa -t rsa -N ''
RUN cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys && chmod 600 ~/.ssh/*

##Flink 0.10.0 Installation
###Download:
RUN mkdir ~/downloads && cd ~/downloads && \
    wget -q -O - http://mirror.switch.ch/mirror/apache/dist/flink/flink-0.10.1/flink-0.10.1-bin-hadoop27-scala_2.11.tgz| tar -zxvf - -C /usr/local/
RUN cd /usr/local && ln -s ./flink-0.10.1 flink

ENV FLINK_HOME /usr/local/flink
ENV PATH $PATH:$FLINK_HOME/bin

#config files (template)
ADD conf/flink-conf.yaml /usr/local/flink/conf/

ADD config-flink.sh /usr/local/flink/bin/
RUN chmod +x /usr/local/flink/bin/config-flink.sh

EXPOSE 6123
EXPOSE 22

CMD ["/usr/local/flink/bin/config-flink.sh", "taskmanager"]
