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
import org.apache.flink.test.parameters.ParameterProperty;
import org.apache.flink.util.DockerImageVersions;

import com.github.dockerjava.api.command.InspectContainerResponse;
import okhttp3.FormBody;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

/** Factory to create the {@link HiveContainer}. */
public class HiveContainers {

    public static final String HOST_NAME = "hadoop-master";
    public static final int HIVE_METASTORE_PORT = 9083;

    private static final boolean HIVE_310_OR_LATER =
            HiveShimLoader.getHiveVersion().compareTo(HiveShimLoader.HIVE_VERSION_V3_1_0) >= 0;
    private static final ParameterProperty<Path> DISTRIBUTION_LOG_BACKUP_DIRECTORY =
            new ParameterProperty<>("logBackupDir", Paths::get);

    public static HiveContainer createHiveContainer(List<String> initTableNames) {
        if (HIVE_310_OR_LATER) {
            return new Hive3Container(initTableNames);
        } else {
            return new Hive2Container(initTableNames);
        }
    }

    /** Test container for Hive. */
    public abstract static class HiveContainer extends GenericContainer<HiveContainer> {

        private static final Logger LOG = LoggerFactory.getLogger(HiveContainer.class);
        protected String hiveWarehouseDir;

        public HiveContainer(String imageName, List<String> initTableNames) {
            super(DockerImageName.parse(imageName));
            withExtraHost(HOST_NAME, "127.0.0.1");
            withStartupAttempts(3);
            addExposedPort(HIVE_METASTORE_PORT);
            addExposedPort(getNameNodeWebEndpointPort());
            mountHiveWarehouseDirToContainer(initTableNames);
        }

        @Override
        protected void doStart() {
            super.doStart();
            if (LOG.isInfoEnabled()) {
                followOutput(new Slf4jLogConsumer(LOG));
            }
        }

        @Override
        protected void finished(Description description) {
            backupLogs();
            super.finished(description);
        }

        @Override
        protected void containerIsStarted(InspectContainerResponse containerInfo) {
            super.containerIsStarted(containerInfo);
            final Request request =
                    new Request.Builder()
                            .post(new FormBody.Builder().build())
                            .url(
                                    String.format(
                                            "http://127.0.0.1:%s",
                                            getMappedPort(getNameNodeWebEndpointPort())))
                            .build();
            OkHttpClient client = new OkHttpClient();
            try (Response response = client.newCall(request).execute()) {
                if (!response.isSuccessful()) {
                    throw new RuntimeException(
                            String.format(
                                    "The rest request is not successful: %s", response.message()));
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
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
                setFilePermission(file);
                hiveWarehouseDir = file.getAbsolutePath();
                LOG.info("mountHiveWarehouseDirToContainer: " + hiveWarehouseDir);

                mount(initTableNames);
            } catch (IOException e) {
                throw new IllegalStateException("Failed to create warehouse directory", e);
            }
        }

        protected void setFilePermission(File file) {
            file.setReadable(true, false);
            file.setWritable(true, false);
            file.setExecutable(true, false);
        }

        private void backupLogs() {
            Path path = DISTRIBUTION_LOG_BACKUP_DIRECTORY.get().orElse(null);
            if (path == null) {
                LOG.warn(
                        "Property {} not set, logs will not be backed up.",
                        DISTRIBUTION_LOG_BACKUP_DIRECTORY.getPropertyName());
                return;
            }
            try {
                Path dir =
                        Files.createDirectory(
                                Paths.get(
                                        String.valueOf(path.toAbsolutePath()),
                                        "hive-" + UUID.randomUUID()));

                for (String filePathInDocker : getBackupLogPaths()) {
                    copyFileFromContainer(
                            filePathInDocker,
                            dir.resolve(Paths.get(filePathInDocker).getFileName()).toString());
                }
            } catch (Throwable e) {
                LOG.warn("Failed to backup logs...", e);
            }
        }

        protected abstract List<String> getBackupLogPaths();

        protected abstract void mount(List<String> initTableNames) throws IOException;

        protected abstract int getNameNodeWebEndpointPort();
    }

    private static class Hive2Container extends HiveContainer {

        private static final String METASTORE_LOG_PATH = "/tmp/hive/hive.log";
        private static final String MYSQL_METASTORE_LOG_PATH = "/var/log/mysqld.log";
        private static final String NAME_NODE_LOG_PATH =
                "/var/log/hadoop-hdfs/hadoop-hdfs-namenode.log";
        private static final int NAME_NODE_WEB_PORT = 50070;

        public Hive2Container(List<String> initTableNames) {
            super(DockerImageVersions.HIVE2, initTableNames);
        }

        @Override
        protected List<String> getBackupLogPaths() {
            // Detailed log paths are from
            // https://github.com/prestodb/docker-images/tree/master/prestodb/hdp2.6-hive/files/etc/supervisord.d
            return Arrays.asList(METASTORE_LOG_PATH, MYSQL_METASTORE_LOG_PATH, NAME_NODE_LOG_PATH);
        }

        @Override
        protected void mount(List<String> initTableNames) {
            withFileSystemBind(hiveWarehouseDir, hiveWarehouseDir);
        }

        @Override
        protected int getNameNodeWebEndpointPort() {
            return NAME_NODE_WEB_PORT;
        }
    }

    private static class Hive3Container extends HiveContainer {

        private static final String METASTORE_LOG_PATH = "/tmp/root/hive.log";
        private static final String MARIADB_METASTORE_LOG_PATH = "/var/log/mariadb/mariadb.log";
        private static final String NAME_NODE_LOG_PATH =
                "/var/log/hadoop-hdfs/hadoop-hdfs-namenode.log";
        private static final int NAME_NODE_WEB_PORT = 9870;

        public Hive3Container(List<String> initTableNames) {
            super(DockerImageVersions.HIVE3, initTableNames);
        }

        @Override
        protected List<String> getBackupLogPaths() {
            // Detailed log paths are from
            // https://github.com/prestodb/docker-images/blob/master/prestodb/hive3.1-hive/files/etc/supervisord.conf
            return Arrays.asList(
                    METASTORE_LOG_PATH, MARIADB_METASTORE_LOG_PATH, NAME_NODE_LOG_PATH);
        }

        @Override
        protected void mount(List<String> initTableNames) throws IOException {
            // we should first create the dir for the table,
            // and set it readable & writable by all, otherwise, it'll throw
            // permission denied exception when try to write the tables
            for (String tableName : initTableNames) {
                File file =
                        Files.createDirectory(Paths.get(hiveWarehouseDir).resolve(tableName))
                                .toFile();
                setFilePermission(file);
            }
            withFileSystemBind(hiveWarehouseDir, hiveWarehouseDir);
        }

        @Override
        protected int getNameNodeWebEndpointPort() {
            return NAME_NODE_WEB_PORT;
        }
    }
}
