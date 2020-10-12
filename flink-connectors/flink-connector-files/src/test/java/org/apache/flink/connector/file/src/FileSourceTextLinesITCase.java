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

package org.apache.flink.connector.file.src;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.reader.TextLineFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.Duration;

/**
 * MiniCluster-based integration test for the {@link FileSource} with text format.
 */
public class FileSourceTextLinesITCase extends AbstractFileSourceITCase {

	@Override
	protected DataStream<String> createSourceStream(
			StreamExecutionEnvironment env, Path path, Duration discoveryInterval) {
		FileSource.FileSourceBuilder<String> builder = FileSource
				.forRecordStreamFormat(new TextLineFormat(), path);
		if (discoveryInterval != null) {
			builder.monitorContinuously(discoveryInterval);
		}
		return env.fromSource(builder.build(), WatermarkStrategy.noWatermarks(), "file-source");
	}

	@Override
	protected void writeFormatFile(File file, String[] lines) throws IOException {
		try (PrintWriter writer = new PrintWriter(new FileWriter(file))) {
			for (String line : lines) {
				writer.println(line);
			}
		}
	}
}
