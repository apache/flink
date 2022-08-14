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

package org.apache.flink.util;

/**
 * Utility class for defining the image names and versions of Docker containers used during the Java
 * tests. The names/versions are centralised here in order to make testing version updates easier,
 * as well as to provide a central file to use as a key when caching testing Docker files.
 */
public class DockerImageVersions {
    public static final String ELASTICSEARCH_7 =
            "docker.elastic.co/elasticsearch/elasticsearch-oss:7.5.1";

    public static final String ELASTICSEARCH_6 =
            "docker.elastic.co/elasticsearch/elasticsearch-oss:6.3.1";

    public static final String KAFKA = "confluentinc/cp-kafka:5.5.2";

    public static final String RABBITMQ = "rabbitmq:3.7.25-management-alpine";

    public static final String KINESALITE = "instructure/kinesalite:latest";

    public static final String PULSAR = "apachepulsar/pulsar:2.9.1";

    public static final String CASSANDRA_3 = "cassandra:3.0";
}
