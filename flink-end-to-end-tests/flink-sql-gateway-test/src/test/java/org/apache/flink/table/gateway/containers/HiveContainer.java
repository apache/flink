/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.gateway.containers;

import org.apache.flink.util.DockerImageVersions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;

/** Test Container for hive. */
public class HiveContainer extends GenericContainer<HiveContainer> {

    private static final Logger LOG = LoggerFactory.getLogger(HiveContainer.class);

    public static final String HOST_NAME = "hadoop-master";
    public static final int HIVE_METASTORE_PORT = 9083;

    public HiveContainer() {
        super(DockerImageName.parse(DockerImageVersions.HIVE2));
        withExtraHost(HOST_NAME, "127.0.0.1");
        addExposedPort(HIVE_METASTORE_PORT);
    }

    @Override
    protected void doStart() {
        super.doStart();
        if (LOG.isInfoEnabled()) {
            followOutput(new Slf4jLogConsumer(LOG));
        }
    }

    public String getHiveMetastoreURI() {
        return String.format("thrift://%s:%s", getHost(), getMappedPort(HIVE_METASTORE_PORT));
    }
}
