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

package org.apache.flink.connectors.hive;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.fs.hdfs.HadoopFileSystem;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.hive.client.HiveShim;
import org.apache.flink.table.filesystem.OutputFormatFactory;
import org.apache.flink.table.functions.hive.conversion.HiveInspectors;
import org.apache.flink.table.functions.hive.conversion.HiveObjectConversion;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.Serializer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Hive {@link OutputFormatFactory}, use {@link RecordWriter} to write record.
 */
public class HiveOutputFormatFactory implements OutputFormatFactory<Row> {

	private static final long serialVersionUID = 1L;

	private final Class hiveOutputFormatClz;

	private final SerDeInfo serDeInfo;

	private final String[] allColumns;

	private final DataType[] allTypes;

	private final String[] partitionColumns;

	private final Properties tableProperties;

	private final JobConfWrapper confWrapper;

	private final HiveShim hiveShim;

	private final boolean isCompressed;

	// number of non-partitioning columns
	private transient int numNonPartitionColumns;

	// SerDe in Hive-1.2.1 and Hive-2.3.4 can be of different classes, make sure to use a common base class
	private transient Serializer recordSerDe;

	// to convert Flink object to Hive object
	private transient HiveObjectConversion[] hiveConversions;

	//StructObjectInspector represents the hive row structure.
	private transient StructObjectInspector rowObjectInspector;

	private transient boolean inited;

	public HiveOutputFormatFactory(
			JobConf jobConf,
			Class hiveOutputFormatClz,
			SerDeInfo serDeInfo,
			TableSchema schema,
			String[] partitionColumns,
			Properties tableProperties,
			HiveShim hiveShim,
			boolean isCompressed) {
		Preconditions.checkArgument(org.apache.hadoop.hive.ql.io.HiveOutputFormat.class.isAssignableFrom(hiveOutputFormatClz),
				"The output format should be an instance of HiveOutputFormat");
		this.confWrapper = new JobConfWrapper(jobConf);
		this.hiveOutputFormatClz = hiveOutputFormatClz;
		this.serDeInfo = serDeInfo;
		this.allColumns = schema.getFieldNames();
		this.allTypes = schema.getFieldDataTypes();
		this.partitionColumns = partitionColumns;
		this.tableProperties = tableProperties;
		this.hiveShim = hiveShim;
		this.isCompressed = isCompressed;
	}

	private void init() throws Exception {
		JobConf jobConf = confWrapper.conf();
		Object serdeLib = Class.forName(serDeInfo.getSerializationLib()).newInstance();
		Preconditions.checkArgument(serdeLib instanceof Serializer && serdeLib instanceof Deserializer,
				"Expect a SerDe lib implementing both Serializer and Deserializer, but actually got "
						+ serdeLib.getClass().getName());
		this.recordSerDe = (Serializer) serdeLib;
		ReflectionUtils.setConf(recordSerDe, jobConf);

		// TODO: support partition properties, for now assume they're same as table properties
		SerDeUtils.initializeSerDe((Deserializer) recordSerDe, jobConf, tableProperties, null);

		this.numNonPartitionColumns = allColumns.length - partitionColumns.length;
		this.hiveConversions = new HiveObjectConversion[numNonPartitionColumns];
		List<ObjectInspector> objectInspectors = new ArrayList<>(hiveConversions.length);
		for (int i = 0; i < numNonPartitionColumns; i++) {
			ObjectInspector objectInspector = HiveInspectors.getObjectInspector(allTypes[i]);
			objectInspectors.add(objectInspector);
			hiveConversions[i] = HiveInspectors.getConversion(objectInspector, allTypes[i].getLogicalType(), hiveShim);
		}

		this.rowObjectInspector = ObjectInspectorFactory.getStandardStructObjectInspector(
				Arrays.asList(allColumns).subList(0, numNonPartitionColumns),
				objectInspectors);
	}

	@Override
	public HiveOutputFormat createOutputFormat(Path outPath) {
		try {
			if (!inited) {
				init();
				inited = true;
			}

			JobConf conf = new JobConf(confWrapper.conf());

			if (isCompressed) {
				String codecStr = conf.get(HiveConf.ConfVars.COMPRESSINTERMEDIATECODEC.varname);
				if (!StringUtils.isNullOrWhitespaceOnly(codecStr)) {
					//noinspection unchecked
					Class<? extends CompressionCodec> codec =
							(Class<? extends CompressionCodec>) Class.forName(codecStr, true,
									Thread.currentThread().getContextClassLoader());
					FileOutputFormat.setOutputCompressorClass(conf, codec);
				}
				String typeStr = conf.get(HiveConf.ConfVars.COMPRESSINTERMEDIATETYPE.varname);
				if (!StringUtils.isNullOrWhitespaceOnly(typeStr)) {
					SequenceFile.CompressionType style = SequenceFile.CompressionType.valueOf(typeStr);
					SequenceFileOutputFormat.setOutputCompressionType(conf, style);
				}
			}

			RecordWriter recordWriter = hiveShim.getHiveRecordWriter(
					conf,
					hiveOutputFormatClz,
					recordSerDe.getSerializedClass(),
					isCompressed,
					tableProperties,
					HadoopFileSystem.toHadoopPath(outPath));
			return new HiveOutputFormat(recordWriter);
		} catch (Exception e) {
			throw new FlinkHiveException(e);
		}
	}

	private class HiveOutputFormat implements org.apache.flink.api.common.io.OutputFormat<Row> {

		private final RecordWriter recordWriter;

		private HiveOutputFormat(RecordWriter recordWriter) {
			this.recordWriter = recordWriter;
		}

		// converts a Row to a list of Hive objects so that Hive can serialize it
		private Object getConvertedRow(Row record) {
			List<Object> res = new ArrayList<>(numNonPartitionColumns);
			for (int i = 0; i < numNonPartitionColumns; i++) {
				res.add(hiveConversions[i].toHiveObject(record.getField(i)));
			}
			return res;
		}

		@Override
		public void configure(Configuration parameters) {
		}

		@Override
		public void open(int taskNumber, int numTasks) throws IOException {
		}

		@Override
		public void writeRecord(Row record) throws IOException {
			try {
				recordWriter.write(recordSerDe.serialize(getConvertedRow(record), rowObjectInspector));
			} catch (SerDeException e) {
				throw new IOException(e);
			}
		}

		@Override
		public void close() throws IOException {
			recordWriter.close(false);
		}
	}
}
