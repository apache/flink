/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.elasticsearch.testutils;

import org.apache.flink.annotation.Internal;

import org.slf4j.Logger;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.utility.DockerImageName;

/** Utility class for creating {@link ElasticsearchContainer}s. */
@Internal
public class ElasticsearchContainerUtil {

    private ElasticsearchContainerUtil() {}

    /**
     * Creates a preconfigured {@link ElasticsearchContainer} that aligns the internal Elasticsearch
     * log levels with the ones used by the capturing logger.
     *
     * @param dockerImageVersion describing the Elasticsearch image
     * @param log to derive the log level from
     * @return configured Elasticsearch container
     */
    public static ElasticsearchContainer createElasticsearchContainer(
            String dockerImageVersion, Logger log) {
        String logLevel;
        if (log.isTraceEnabled()) {
            logLevel = "TRACE";
        } else if (log.isDebugEnabled()) {
            logLevel = "DEBUG";
        } else if (log.isInfoEnabled()) {
            logLevel = "INFO";
        } else if (log.isWarnEnabled()) {
            logLevel = "WARN";
        } else if (log.isErrorEnabled()) {
            logLevel = "ERROR";
        } else {
            logLevel = "OFF";
        }

        return new ElasticsearchContainer(DockerImageName.parse(dockerImageVersion))
                .withEnv("logger.org.elasticsearch", logLevel)
                .withLogConsumer(new Slf4jLogConsumer(log));
    }
}
