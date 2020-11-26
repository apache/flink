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

package org.apache.flink.tests.util.hbase;

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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * {@link HBaseResource} that downloads hbase and set up a local hbase cluster.
 */
public class LocalStandaloneHBaseResource implements HBaseResource {

	private static final Logger LOG = LoggerFactory.getLogger(LocalStandaloneHBaseResource.class);

	private static final int MAX_RETRIES = 3;
	private static final int RETRY_INTERVAL_SECONDS = 30;
	private final TemporaryFolder tmp = new TemporaryFolder();

	private final DownloadCache downloadCache = DownloadCache.get();
	private final String hbaseVersion;
	private Path hbaseDir;

	LocalStandaloneHBaseResource(String hbaseVersion) {
		OperatingSystemRestriction.forbid(
			String.format("The %s relies on UNIX utils and shell scripts.", getClass().getSimpleName()),
			OperatingSystem.WINDOWS);
		this.hbaseVersion = hbaseVersion;
	}

	private String getHBaseDownloadUrl() {
		return String.format("https://archive.apache.org/dist/hbase/%1$s/hbase-%1$s-bin.tar.gz", hbaseVersion);
	}

	@Override
	public void before() throws Exception {
		tmp.create();
		downloadCache.before();

		this.hbaseDir = tmp.newFolder("hbase-" + hbaseVersion).toPath().toAbsolutePath();
		setupHBaseDist();
		setupHBaseCluster();
	}

	private void setupHBaseDist() throws IOException {
		final Path downloadDirectory = tmp.newFolder("getOrDownload").toPath();
		final Path hbaseArchive = downloadCache.getOrDownload(getHBaseDownloadUrl(), downloadDirectory);

		LOG.info("HBase location: {}", hbaseDir.toAbsolutePath());
		AutoClosableProcess.runBlocking(CommandLineWrapper
			.tar(hbaseArchive)
			.extract()
			.zipped()
			.strip(1)
			.targetDir(hbaseDir)
			.build());

		LOG.info("Configure {} as hbase.tmp.dir", hbaseDir.toAbsolutePath());
		final String tmpDirConfig = "<configuration><property><name>hbase.tmp.dir</name><value>" + hbaseDir + "</value></property></configuration>";
		Files.write(hbaseDir.resolve(Paths.get("conf", "hbase-site.xml")), tmpDirConfig.getBytes());
	}

	private void setupHBaseCluster() throws IOException {
		LOG.info("Starting HBase cluster...");
		runHBaseProcessWithRetry("start-hbase.sh", () -> !isHMasterRunning());
		LOG.info("Start HBase cluster success");
	}

	@Override
	public void afterTestSuccess() {
		shutdownResource();
		downloadCache.afterTestSuccess();
		tmp.delete();
	}

	private void shutdownResource() {
		LOG.info("Stopping HBase Cluster...");
		try {
			runHBaseProcessWithRetry("stop-hbase.sh", () -> isHMasterAlive());
		} catch (IOException ioe) {
			LOG.warn("Error when shutting down HBase Cluster.", ioe);
		}
		LOG.info("Stop HBase Cluster success");
	}

	private void runHBaseProcessWithRetry(String command, Supplier<Boolean> processStatusChecker) throws IOException {
		LOG.info("Execute {} for HBase Cluster", command);

		for (int i = 1; i <= MAX_RETRIES; i++) {
			try {
				AutoClosableProcess.runBlocking(
						hbaseDir.resolve(Paths.get("bin", command)).toString());
			} catch (IOException ioe) {
				LOG.warn("Get exception when execute {} ", command, ioe);
			}

			int waitSecond = 0;
			while (processStatusChecker.get()) {
				try {
					LOG.info("Waiting for HBase {} works", command);
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
					throw new IllegalArgumentException(String.format(
							"Execute %s failed aftert retry %s times", command, i));
				} else {
					LOG.warn("Execute {} failed, retry times {}", command, i);
				}
			}
		}
	}

	private boolean isHMasterRunning() {
		try {
			final AtomicBoolean atomicHMasterStarted = new AtomicBoolean(false);
			queryHBaseStatus(line ->
					atomicHMasterStarted.compareAndSet(false, line.contains("hbase:namespace")));
			return atomicHMasterStarted.get();
		} catch (IOException ioe) {
			return false;
		}
	}

	private void queryHBaseStatus(final Consumer<String> stdoutProcessor) throws IOException {
		executeHBaseShell("scan 'hbase:meta'", stdoutProcessor);
	}

	private boolean isHMasterAlive() {
		try {
			final AtomicBoolean atomicHMasterStarted = new AtomicBoolean(false);
			queryHBaseProcess(line ->
					atomicHMasterStarted.compareAndSet(false, line.contains("HMaster")));
			return atomicHMasterStarted.get();
		} catch (IOException ioe) {
			return false;
		}
	}

	private void queryHBaseProcess(final Consumer<String> stdoutProcessor) throws IOException {
		AutoClosableProcess
				.create("jps")
				.setStdoutProcessor(stdoutProcessor)
				.runBlocking();
	}

	@Override
	public void createTable(String tableName, String... columnFamilies) throws IOException {
		final String createTable = String.format("create '%s',", tableName) +
			Arrays.stream(columnFamilies)
				.map(cf -> String.format("{NAME=>'%s'}", cf))
				.collect(Collectors.joining(","));

		executeHBaseShell(createTable);
	}

	@Override
	public List<String> scanTable(String tableName) throws IOException {
		final List<String> result = new ArrayList<>();
		executeHBaseShell(String.format("scan '%s'", tableName), line -> {
			if (line.contains("value=")) {
				result.add(line);
			}
		});
		return result;
	}

	@Override
	public void putData(String tableName, String rowKey, String columnFamily, String columnQualifier, String value) throws IOException {
		executeHBaseShell(
			String.format("put '%s','%s','%s:%s','%s'", tableName, rowKey, columnFamily, columnQualifier, value));
	}

	private void executeHBaseShell(String cmd) throws IOException {
		executeHBaseShell(cmd, line -> {
		});
	}

	private void executeHBaseShell(String cmd, Consumer<String> stdoutProcessor) throws IOException {
		AutoClosableProcess
			.create(hbaseDir.resolve(Paths.get("bin", "hbase")).toString(), "shell")
			.setStdoutProcessor(stdoutProcessor)
			.setStdInputs(cmd)
			.runBlocking();
	}
}
