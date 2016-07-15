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

FROM java:8-jre-alpine

# Install requirements
RUN apk add --no-cache bash snappy

# Configure Flink version
ARG FLINK_VERSION=1.0.3
ARG HADOOP_VERSION=27
ARG SCALA_VERSION=2.11

# Install build dependencies and flink
RUN set -x && \
  apk --update add --virtual build-dependencies curl && \
  curl -s $(curl -s https://www.apache.org/dyn/closer.cgi\?as_json\=1 | \
  awk '/preferred/ {gsub(/"/,""); print $2}')flink/flink-${FLINK_VERSION}/flink-${FLINK_VERSION}-bin-hadoop${HADOOP_VERSION}-scala_${SCALA_VERSION}.tgz | \
  tar xvz -C /usr/local/ && \
  ln -s /usr/local/flink-$FLINK_VERSION /usr/local/flink && \
  sed -i -e "s/echo \$mypid >> \$pid/echo \$mypid >> \$pid \&\& wait/g" /usr/local/flink/bin/flink-daemon.sh && \
  apk del build-dependencies && \
  rm -rf /var/cache/apk/*

ENV FLINK_HOME /usr/local/flink
ENV PATH $PATH:$FLINK_HOME/bin

# Configure container
ADD docker-entrypoint.sh $FLINK_HOME/bin/
ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["sh", "-c"]
