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

FROM openjdk:8-jre-alpine

# Install requirements
RUN apk add --no-cache bash snappy

# Configure Flink version
ARG FLINK_VERSION=1.2.0
ARG HADOOP_VERSION=27
ARG SCALA_VERSION=2.11

# Flink environment variables
ENV FLINK_INSTALL_PATH=/opt
ENV FLINK_HOME=$FLINK_INSTALL_PATH/flink
ENV PATH=$PATH:$FLINK_HOME/bin
ENV FLINK_USER=flink
ENV FLINK_UID=9999

RUN addgroup -S -g $FLINK_UID $FLINK_USER && \
  adduser -D -S -H -u $FLINK_UID -G $FLINK_USER -h $FLINK_HOME flink

# Install build dependencies and flink
RUN set -x && \
  mkdir -p $FLINK_INSTALL_PATH && \
  apk --update add --virtual build-dependencies curl && \
  curl -s $(curl -s https://www.apache.org/dyn/closer.cgi\?preferred\=true)flink/flink-${FLINK_VERSION}/flink-${FLINK_VERSION}-bin-hadoop${HADOOP_VERSION}-scala_${SCALA_VERSION}.tgz | \
  tar xvz -C $FLINK_INSTALL_PATH && \
  ln -s $FLINK_INSTALL_PATH/flink-$FLINK_VERSION $FLINK_HOME && \
  chown -R $FLINK_USER:$FLINK_USER $FLINK_INSTALL_PATH/flink-$FLINK_VERSION && \
  chown -h $FLINK_USER:$FLINK_USER $FLINK_HOME && \
  sed -i -e "s/echo \$mypid >> \$pid/echo \$mypid >> \$pid \&\& wait/g" $FLINK_HOME/bin/flink-daemon.sh && \
  apk del build-dependencies && \
  rm -rf /var/cache/apk/*

# Configure container
USER flink
ADD docker-entrypoint.sh $FLINK_HOME/bin/
ENTRYPOINT ["docker-entrypoint.sh"]
# These can be mapped from the host to the container using
# $ docker run -t flink -p 8081:8081 -p 6123:6123 jobmanager
# 6123: jobmanager.rpc.port
# 8081: jobmanager.web.port
EXPOSE 6123 8081
CMD ["sh", "-c"]
