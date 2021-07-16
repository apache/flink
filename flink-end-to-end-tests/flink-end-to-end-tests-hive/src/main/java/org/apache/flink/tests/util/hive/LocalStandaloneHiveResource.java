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

package org.apache.flink.tests.util.hive;

import org.apache.flink.tests.util.AutoClosableProcess;
import org.apache.flink.tests.util.CommandLineWrapper;
import org.apache.flink.tests.util.activation.OperatingSystemRestriction;
import org.apache.flink.tests.util.cache.DownloadCache;
import org.apache.flink.util.OperatingSystem;

import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;

/** {@link HiveResource} that downloads hbase and set up a local hbase cluster. */
public class LocalStandaloneHiveResource implements HiveResource {

    private static final Logger LOG = LoggerFactory.getLogger(LocalStandaloneHiveResource.class);

    private static final int MAX_RETRIES = 3;
    private static final int RETRY_INTERVAL_SECONDS = 30;
    private final TemporaryFolder tmp = new TemporaryFolder();

    private final DownloadCache downloadCache = DownloadCache.get();
    private final String hiveVersion;
    private Path hiveDir;

    LocalStandaloneHiveResource(String hiveVersion) {
        OperatingSystemRestriction.forbid(
                String.format(
                        "The %s relies on UNIX utils and shell scripts.",
                        getClass().getSimpleName()),
                OperatingSystem.WINDOWS);
        this.hiveVersion = hiveVersion;
    }

    private String getHiveDownloadUrl() {
        return String.format(
                "https://archive.apache.org/dist/hive/%1$s/apache-hive-%1$s-bin.tar.gz",
                hiveVersion);
    }

    @Override
    public void before() throws Exception {
        tmp.create();
        downloadCache.before();

        this.hiveDir = tmp.newFolder("apache-hive-" + hiveVersion).toPath().toAbsolutePath();
        setupHiveDist();
        setupHBaseCluster();
    }

    private void setupHiveDist() throws IOException {
        final Path downloadDirectory = tmp.newFolder("getOrDownload").toPath();
        final Path hiveArchive =
                downloadCache.getOrDownload(getHiveDownloadUrl(), downloadDirectory);

        LOG.info("Hive location: {}", hiveDir.toAbsolutePath());
        AutoClosableProcess.runBlocking(
                CommandLineWrapper.tar(hiveArchive)
                        .extract()
                        .zipped()
                        .strip(1)
                        .targetDir(hiveDir)
                        .build());

        LOG.info("Configure {} hive configuration", hiveDir.toAbsolutePath());
        final String tmpDirConfig = "<configuration></configuration>";
        Files.write(hiveDir.resolve(Paths.get("conf", "hive-site.xml")), tmpDirConfig.getBytes());
    }

    private void setupHBaseCluster() {
        LOG.info("Starting hive metastore...");
        runHiveProcessWithRetry("hive --service metastore &", () -> isHiveAlive());
        LOG.info("Start hive metastore success");
    }

    @Override
    public void afterTestSuccess() {
        shutdownResource();
        downloadCache.afterTestSuccess();
        tmp.delete();
    }

    private void shutdownResource() {
        LOG.info("Stopping hive metastore...");
        runHiveProcessWithRetry("jps -lvmm|grep HiveMetaStore|xargs kill -9", () -> !isHiveAlive());
        LOG.info("Stop hive metastore success");
    }

    private void runHiveProcessWithRetry(String command, Supplier<Boolean> processStatusChecker) {
        LOG.info("Execute {} for hive Cluster", command);

        for (int i = 1; i <= MAX_RETRIES; i++) {
            try {
                AutoClosableProcess.runBlocking(
                        hiveDir.resolve(Paths.get("bin", command)).toString());
            } catch (IOException ioe) {
                LOG.warn("Get exception when execute {} ", command, ioe);
            }

            int waitSecond = 0;
            while (processStatusChecker.get()) {
                try {
                    LOG.info("Waiting for hive {} works", command);
                    Thread.sleep(1000L);
                } catch (InterruptedException e) {
                    LOG.warn("sleep interrupted", e);
                }
                waitSecond++;
                if (waitSecond > RETRY_INTERVAL_SECONDS) {
                    break;
                }
            }

            if (waitSecond < RETRY_INTERVAL_SECONDS) {
                break;
            } else {
                if (i == MAX_RETRIES) {
                    LOG.error("Execute {} failed, retry times {}", command, i);
                    throw new IllegalArgumentException(
                            String.format("Execute %s failed aftert retry %s times", command, i));
                } else {
                    LOG.warn("Execute {} failed, retry times {}", command, i);
                }
            }
        }
    }

    private boolean isHiveAlive() {
        try {
            final AtomicBoolean atomicHMasterStarted = new AtomicBoolean(false);
            queryHiveProcess(
                    line ->
                            atomicHMasterStarted.compareAndSet(
                                    false, line.contains("HiveMetaStore")));
            return atomicHMasterStarted.get();
        } catch (IOException ioe) {
            return false;
        }
    }

    private void queryHiveProcess(final Consumer<String> stdoutProcessor) throws IOException {
        AutoClosableProcess.create("jps -lvmm").setStdoutProcessor(stdoutProcessor).runBlocking();
    }

    @Override
    public void createCatalog(String catalogName) {}

    @Override
    public void createTable(String tableInfo) throws IOException {}

    @Override
    public String getDatabase() {
        return null;
    }
}
