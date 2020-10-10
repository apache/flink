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

package org.apache.flink.formats.parquet;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.file.src.AbstractFileSourceITCase;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.utils.ParquetWriterUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.Row;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.parquet.schema.Types.primitive;

/**
 * MiniCluster-based integration test for the {@link ParquetColumnarRowInputFormat}.
 */
public class FileSourceParquetColumnarITCase extends AbstractFileSourceITCase {

	@Override
	protected DataStream<String> createSourceStream(
			StreamExecutionEnvironment env, Path path, Duration discoveryInterval) {
		FileSource.FileSourceBuilder<RowData> builder = FileSource
				.forBulkFileFormat(new ParquetColumnarRowInputFormat(
						new Configuration(),
						new String[]{"f0"},
						new LogicalType[] {DataTypes.STRING().getLogicalType()},
						20,
						false,
						false), path);
		if (discoveryInterval != null) {
			builder.monitorContinuously(discoveryInterval);
		}
		return env.fromSource(
				builder.build(),
				WatermarkStrategy.noWatermarks(),
				"file-source")
				.map((MapFunction<RowData, String>) value -> value.getString(0).toString());
	}

	@Override
	protected void writeFormatFile(File file, String[] lines) throws IOException {
		List<Row> rows = Arrays.stream(lines).map(Row::of).collect(Collectors.toList());
		MessageType messageType = new MessageType(
				"row", primitive(PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.REQUIRED).named("f0"));
		ParquetWriterUtil.writeParquetFile(Path.fromLocalFile(file), messageType, rows, 20);
	}
}
