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
ENV FLINK_VERSION=1.1.1
ENV HADOOP_VERSION=27
ENV SCALA_VERSION=2.11

# Flink environment variables
ENV FLINK_INSTALL_PATH=/opt
ENV FLINK_HOME $FLINK_INSTALL_PATH/flink
ENV PATH $PATH:$FLINK_HOME/bin

# These can be mapped from the host to the container using
# $ docker run -t flink -p 8081:8081 -p 6123:6123 jobmanager
EXPOSE 8081
EXPOSE 6123

# Install build dependencies and flink
RUN set -x && \
  mkdir -p $FLINK_INSTALL_PATH && \
  apk --update add --virtual build-dependencies curl && \
  curl -s $(curl -s https://www.apache.org/dyn/closer.cgi\?preferred\=true)flink/flink-${FLINK_VERSION}/flink-${FLINK_VERSION}-bin-hadoop${HADOOP_VERSION}-scala_${SCALA_VERSION}.tgz | \
  tar xvz -C $FLINK_INSTALL_PATH && \
  ln -s $FLINK_INSTALL_PATH/flink-$FLINK_VERSION $FLINK_HOME && \
  addgroup -S flink && adduser -D -S -H -G flink -h $FLINK_HOME flink && \
  chown -R flink:flink $FLINK_INSTALL_PATH/flink-$FLINK_VERSION && \
  chown -h flink:flink $FLINK_HOME && \
  sed -i -e "s/echo \$mypid >> \$pid/echo \$mypid >> \$pid \&\& wait/g" $FLINK_HOME/bin/flink-daemon.sh && \
  apk del build-dependencies && \
  rm -rf /var/cache/apk/*

# Configure container
USER flink
ADD docker-entrypoint.sh $FLINK_HOME/bin/
ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["sh", "-c"]
