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
import java.util.stream.Collectors;

/**
 * {@link HBaseResource} that downloads hbase and set up a local hbase cluster.
 */
public class LocalStandaloneHBaseResource implements HBaseResource {

	private static final Logger LOG = LoggerFactory.getLogger(LocalStandaloneHBaseResource.class);

	private final TemporaryFolder tmp = new TemporaryFolder();

	private final DownloadCache downloadCache = DownloadCache.get();
	private Path hbaseDir;

	LocalStandaloneHBaseResource() {
		OperatingSystemRestriction.forbid(
			String.format("The %s relies on UNIX utils and shell scripts.", getClass().getSimpleName()),
			OperatingSystem.WINDOWS);
	}

	private static String getHBaseDownloadUrl() {
		return "https://archive.apache.org/dist/hbase/1.4.3/hbase-1.4.3-bin.tar.gz";
	}

	@Override
	public void before() throws Exception {
		tmp.create();
		downloadCache.before();

		this.hbaseDir = tmp.newFolder("hbase").toPath().toAbsolutePath();
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
		LOG.info("Starting HBase cluster");
		AutoClosableProcess.runBlocking(
			hbaseDir.resolve(Paths.get("bin", "start-hbase.sh")).toString());

		while (!isHBaseRunning()) {
			try {
				LOG.info("Waiting for HBase to start");
				Thread.sleep(500L);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				break;
			}
		}
	}

	@Override
	public void afterTestSuccess() {
		try {
			LOG.info("Stopping HBase Cluster");
			AutoClosableProcess.runBlocking(
				hbaseDir.resolve(Paths.get("bin", "hbase-daemon.sh")).toString(),
				"stop",
				"master");

			while (isHBaseRunning()) {
				try {
					LOG.info("Waiting for HBase to stop");
					Thread.sleep(500L);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					break;
				}
			}

		} catch (IOException ioe) {
			LOG.warn("Error while shutting down hbase.", ioe);
		}
		downloadCache.afterTestSuccess();
		tmp.delete();
	}

	private static boolean isHBaseRunning() {
		try {
			final AtomicBoolean atomicHMasterStarted = new AtomicBoolean(false);
			queryHMasterStatus(line -> atomicHMasterStarted.compareAndSet(false, line.contains("HMaster")));
			return atomicHMasterStarted.get();
		} catch (IOException ioe) {
			return false;
		}
	}

	private static void queryHMasterStatus(final Consumer<String> stdoutProcessor) throws IOException {
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
