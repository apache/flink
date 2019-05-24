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

package org.apache.flink.batch.connectors.hive;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.hive.HMSClientFactory;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.sinks.BatchTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.type.InternalType;
import org.apache.flink.table.type.TypeConverters;
import org.apache.flink.table.typeutils.BaseRowTypeInfo;

import org.apache.flink.shaded.guava18.com.google.common.base.Preconditions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.mapred.JobConf;
import org.apache.thrift.TException;

import java.io.IOException;
import java.util.List;

/**
 * A table sink to write to Hive tables.
 */
public class HiveTableSink implements BatchTableSink<BaseRow> {

	private final JobConf jobConf;
	private final RowTypeInfo rowTypeInfo;
	private final String dbName;
	private final String tableName;
	private final List<String> partitionCols;

	public HiveTableSink(JobConf jobConf, RowTypeInfo rowTypeInfo, String dbName,
						String tableName, List<String> partitionCols) {
		this.jobConf = jobConf;
		this.rowTypeInfo = rowTypeInfo;
		this.dbName = dbName;
		this.tableName = tableName;
		this.partitionCols = partitionCols;
	}

	@Override
	public DataStreamSink<?> emitBoundedStream(DataStream<BaseRow> boundedStream, TableConfig tableConfig, ExecutionConfig executionConfig) {
		// TODO: support partitioning
		final boolean isPartitioned = false;
		// TODO: support overwrite
		final boolean overwrite = false;
		HiveTablePartition hiveTablePartition;
		HiveTableOutputFormat outputFormat;
		IMetaStoreClient client = HMSClientFactory.create(new HiveConf(jobConf, HiveConf.class));
		try {
			Table table = client.getTable(dbName, tableName);
			StorageDescriptor sd = table.getSd();
			// here we use the sdLocation to store the output path of the job, which is always a staging dir
			String sdLocation = sd.getLocation();
			if (isPartitioned) {
				// TODO: implement this
			} else {
				sd.setLocation(toStagingDir(sdLocation, jobConf));
				hiveTablePartition = new HiveTablePartition(sd, null);
			}
			outputFormat = new HiveTableOutputFormat(jobConf, dbName, tableName, partitionCols,
				rowTypeInfo, hiveTablePartition, MetaStoreUtils.getTableMetadata(table), overwrite);
		} catch (TException e) {
			throw new CatalogException("Failed to query Hive metastore", e);
		} catch (IOException e) {
			throw new RuntimeException("Failed to create staging dir", e);
		} finally {
			client.close();
		}
		return boundedStream.writeUsingOutputFormat(outputFormat);
	}

	@Override
	public TypeInformation<BaseRow> getOutputType() {
		InternalType[] internalTypes = new InternalType[rowTypeInfo.getArity()];
		for (int i = 0; i < internalTypes.length; i++) {
			internalTypes[i] = TypeConverters.createInternalTypeFromTypeInfo(rowTypeInfo.getTypeAt(i));
		}
		return new BaseRowTypeInfo(internalTypes, rowTypeInfo.getFieldNames());
	}

	@Override
	public String[] getFieldNames() {
		return rowTypeInfo.getFieldNames();
	}

	@Override
	public TypeInformation<?>[] getFieldTypes() {
		return rowTypeInfo.getFieldTypes();
	}

	@Override
	public TableSink<BaseRow> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
		return new HiveTableSink(jobConf, rowTypeInfo, dbName, tableName, partitionCols);
	}

	// get a staging dir associated with a final dir
	private String toStagingDir(String finalDir, Configuration conf) throws IOException {
		String res = finalDir;
		if (!finalDir.endsWith(Path.SEPARATOR)) {
			res += Path.SEPARATOR;
		}
		// TODO: may append something more meaningful than a timestamp, like query ID
		res += ".staging_" + System.currentTimeMillis();
		Path path = new Path(res);
		FileSystem fs = path.getFileSystem(conf);
		Preconditions.checkState(fs.exists(path) || fs.mkdirs(path), "Failed to create staging dir " + path);
		fs.deleteOnExit(path);
		return res;
	}
}
