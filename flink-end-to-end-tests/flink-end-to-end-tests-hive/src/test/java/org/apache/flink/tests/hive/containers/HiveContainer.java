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

package org.apache.flink.tests.hive.containers;

import org.apache.flink.table.catalog.hive.client.HiveShimLoader;
import org.apache.flink.util.DockerImageVersions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/** Test container for Hive. */
public class HiveContainer extends GenericContainer<HiveContainer> {

    private static final Logger LOG = LoggerFactory.getLogger(HiveContainer.class);
    public static final String HOST_NAME = "hadoop-master";

    private static final boolean HIVE_310_OR_LATER =
            HiveShimLoader.getHiveVersion().compareTo(HiveShimLoader.HIVE_VERSION_V3_1_0) >= 0;
    private String hiveWarehouseDir;

    public static final int HIVE_METASTORE_PORT = 9083;

    public HiveContainer(List<String> initTableNames) {
        super(
                DockerImageName.parse(
                        HIVE_310_OR_LATER ? DockerImageVersions.HIVE3 : DockerImageVersions.HIVE2));
        withExtraHost(HOST_NAME, "127.0.0.1");
        addExposedPort(HIVE_METASTORE_PORT);
        mountHiveWarehouseDirToContainer(initTableNames);
    }

    @Override
    protected void doStart() {
        super.doStart();
        if (LOG.isInfoEnabled()) {
            followOutput(new Slf4jLogConsumer(LOG));
        }
    }

    public String getHiveMetastoreURL() {
        return String.format("thrift://%s:%s", getHost(), getMappedPort(HIVE_METASTORE_PORT));
    }

    public String getWarehousePath() {
        return hiveWarehouseDir;
    }

    private void mountHiveWarehouseDirToContainer(List<String> initTableNames) {
        try {
            Path warehousePath = Files.createTempDirectory("hive_warehouse");
            File file = warehousePath.toFile();
            hiveWarehouseDir = file.getAbsolutePath();
            LOG.info("mountHiveWarehouseDirToContainer: " + hiveWarehouseDir);

            if (HIVE_310_OR_LATER) {
                // if it's hive 3.1+, we should first create the dir for the table,
                // and set it readable & writable by all, otherwise, it'll throw
                // permission denied exception when try to write the tables
                for (String tableName : initTableNames) {
                    file = Files.createDirectory(warehousePath.resolve(tableName)).toFile();
                    setFilePermission(file);
                }
            }

            withFileSystemBind(
                    warehousePath.toAbsolutePath().toString(),
                    warehousePath.toAbsolutePath().toString());
        } catch (IOException e) {
            throw new IllegalStateException("Failed to create warehouse directory", e);
        }
    }

    private void setFilePermission(File file) {
        file.setReadable(true, false);
        file.setWritable(true, false);
    }
}
