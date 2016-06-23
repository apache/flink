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

# Add passless key to ssh
RUN ssh-keygen -f ~/.ssh/id_rsa -t rsa -N ''
RUN cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys && chmod 600 ~/.ssh/*

# Flink Installation
ENV FLINK_VERSION=1.0.3 HADOOP_VERSION=27 SCALA_VERSION=2.11
RUN curl -s $(curl -s https://www.apache.org/dyn/closer.cgi\?as_json\=1 | awk '/preferred/ {gsub(/"/,""); print $2}')flink/flink-${FLINK_VERSION}/flink-${FLINK_VERSION}-bin-hadoop${HADOOP_VERSION}-scala_${SCALA_VERSION}.tgz | tar xvz -C /usr/local/
RUN ln -s /usr/local/flink-$FLINK_VERSION /usr/local/flink
ENV FLINK_HOME=/usr/local/flink PATH=$PATH:$FLINK_HOME/bin

# Config files (template)
ADD conf/flink-conf.yaml $FLINK_HOME/conf/
ADD config-flink.sh $FLINK_HOME/bin/

EXPOSE 22 6121 6122 6123 8081
CMD ["sh", "-c", "${FLINK_HOME}/bin/config-flink.sh", "taskmanager"]
#CMD ["${FLINK_HOME}/bin/config-flink.sh", "taskmanager"]
#CMD ["/usr/local/bin/config-flink.sh", "taskmanager"]
