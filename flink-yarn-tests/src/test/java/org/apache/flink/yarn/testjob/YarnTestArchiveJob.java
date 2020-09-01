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

package org.apache.flink.yarn.testjob;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.yarn.configuration.YarnConfigOptions;

import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableList;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.compress.utils.IOUtils;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Testing job for localizing resources of LocalResourceType.ARCHIVE in per job cluster mode.
 */
public class YarnTestArchiveJob {
	private static final List<String> LIST = ImmutableList.of("test1", "test2");

	private static final Map<String, String> srcFiles = new HashMap<String, String>() {{
			put("local1.txt", "Local text Content1");
			put("local2.txt", "Local text Content2");
		}};

	private static void archiveFilesInDirectory(File directory, String target) throws IOException {
		for (Map.Entry<String, String> entry : srcFiles.entrySet()) {
			Files.write(
					Paths.get(directory.getAbsolutePath() + File.separator + entry.getKey()),
					entry.getValue().getBytes());
		}

		try (FileOutputStream fos = new FileOutputStream(target);
				GzipCompressorOutputStream gos = new GzipCompressorOutputStream(new BufferedOutputStream(fos));
				TarArchiveOutputStream taros = new TarArchiveOutputStream(gos)) {

			taros.setLongFileMode(TarArchiveOutputStream.LONGFILE_GNU);
			for (File f : directory.listFiles()) {
				taros.putArchiveEntry(new TarArchiveEntry(f, directory.getName() + File.separator + f.getName()));

				try (FileInputStream fis = new FileInputStream(f); BufferedInputStream bis = new BufferedInputStream(fis)) {
					IOUtils.copy(bis, taros);
					taros.closeArchiveEntry();
				}
			}
		}

		for (Map.Entry<String, String> entry : srcFiles.entrySet()) {
			Files.delete(Paths.get(directory.getAbsolutePath() + File.separator + entry.getKey()));
		}

	}

	public static JobGraph getArchiveJobGraph(File testDirectory, Configuration config) throws IOException {

		final String archive = testDirectory.getAbsolutePath().concat(".tar.gz");
		archiveFilesInDirectory(testDirectory, archive);
		config.set(YarnConfigOptions.SHIP_ARCHIVES, Collections.singletonList(archive));

		final String localizedPath = testDirectory.getName().concat(".tar.gz") + File.separator + testDirectory.getName();

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		env.addSource(new SourceFunctionWithArchive<>(LIST, localizedPath, TypeInformation.of(String.class)))
			.addSink(new DiscardingSink<>());

		return env.getStreamGraph().getJobGraph();
	}

	private static class SourceFunctionWithArchive<T> extends RichSourceFunction<T> implements ResultTypeQueryable<T> {

		private final List<T> inputDataset;
		private final String resourcePath;
		private final TypeInformation<T> returnType;

		SourceFunctionWithArchive(List<T> inputDataset, String resourcePath, TypeInformation<T> returnType) {
			this.inputDataset = inputDataset;
			this.resourcePath = resourcePath;
			this.returnType = returnType;
		}

		public void open(Configuration parameters) throws Exception {
			for (Map.Entry<String, String> entry : srcFiles.entrySet()) {
				Path path = Paths.get(resourcePath + File.separator + entry.getKey());
				String content = new String(Files.readAllBytes(path));
				checkArgument(entry.getValue().equals(content), "The content of the unpacked file should be identical to the original file's.");
			}
		}

		@Override
		public void run(SourceContext<T> ctx) {
			for (T t : inputDataset) {
				synchronized (ctx.getCheckpointLock()) {
					ctx.collect(t);
				}
			}
		}

		@Override
		public void cancel() {}

		@Override
		public TypeInformation<T> getProducedType() {
			return this.returnType;
		}
	}
}
