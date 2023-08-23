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

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;

/** Test Container for hive. */
public class HiveContainer extends GenericContainer<HiveContainer> {

    private static final Logger LOG = LoggerFactory.getLogger(HiveContainer.class);

    public static final String HOST_NAME = "hadoop-master";
    public static final int HIVE_METASTORE_PORT = 9083;
    private static final int NAME_NODE_WEB_PORT = 50070;

    // Detailed log paths are from
    // https://github.com/prestodb/docker-images/tree/master/prestodb/hdp2.6-hive/files/etc/supervisord.d
    private static final String NAME_NODE_LOG_PATH =
            "/var/log/hadoop-hdfs/hadoop-hdfs-namenode.log";
    private static final String METASTORE_LOG_PATH = "/tmp/hive/hive.log";
    private static final String MYSQL_METASTORE_LOG_PATH = "/var/log/mysqld.log";

    private static final ParameterProperty<Path> DISTRIBUTION_LOG_BACKUP_DIRECTORY =
            new ParameterProperty<>("logBackupDir", Paths::get);

    public HiveContainer() {
        super(DockerImageName.parse(DockerImageVersions.HIVE2));
        withExtraHost(HOST_NAME, "127.0.0.1");
        withStartupAttempts(3);
        addExposedPort(HIVE_METASTORE_PORT);
        addExposedPort(NAME_NODE_WEB_PORT);
    }

    @Override
    protected void doStart() {
        super.doStart();
        if (LOG.isInfoEnabled()) {
            followOutput(new Slf4jLogConsumer(LOG));
        }
    }

    @Override
    protected void containerIsStarted(InspectContainerResponse containerInfo) {
        super.containerIsStarted(containerInfo);
        final Request request =
                new Request.Builder()
                        .post(new FormBody.Builder().build())
                        .url(
                                String.format(
                                        "http://127.0.0.1:%s", getMappedPort(NAME_NODE_WEB_PORT)))
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

    @Override
    protected void finished(Description description) {
        backupLogs();
        super.finished(description);
    }

    public String getHiveMetastoreURI() {
        return String.format("thrift://%s:%s", getHost(), getMappedPort(HIVE_METASTORE_PORT));
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
            copyFileFromContainer(
                    NAME_NODE_LOG_PATH,
                    Files.createFile(Paths.get(dir.toAbsolutePath().toString(), "namenode.log"))
                            .toAbsolutePath()
                            .toString());
            copyFileFromContainer(
                    METASTORE_LOG_PATH,
                    Files.createFile(Paths.get(dir.toAbsolutePath().toString(), "metastore.log"))
                            .toAbsolutePath()
                            .toString());
            copyFileFromContainer(
                    MYSQL_METASTORE_LOG_PATH,
                    Files.createFile(Paths.get(dir.toAbsolutePath().toString(), "mysql.log"))
                            .toAbsolutePath()
                            .toString());
        } catch (Throwable e) {
            LOG.warn("Failed to backup logs...", e);
        }
    }
}
