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

package org.apache.flink.connectors.hive.read;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.io.LocatableInputSplitAssigner;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.java.hadoop.common.HadoopInputFormatCommonBase;
import org.apache.flink.connectors.hive.FlinkHiveException;
import org.apache.flink.connectors.hive.HiveTablePartition;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.hive.client.HiveShimLoader;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.types.DataType;

import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.hadoop.mapreduce.lib.input.FileInputFormat.INPUT_DIR;

/**
 * The HiveTableInputFormat are inspired by the HCatInputFormat and HadoopInputFormatBase.
 * It's used to read from hive partition/non-partition table.
 */
public class HiveTableInputFormat extends HadoopInputFormatCommonBase<BaseRow, HiveTableInputSplit> {

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(HiveTableInputFormat.class);

	private JobConf jobConf;

	private String hiveVersion;

	private List<String> partitionKeys;

	private DataType[] fieldTypes;

	private String[] fieldNames;

	//For non-partition hive table, partitions only contains one partition which partitionValues is empty.
	private List<HiveTablePartition> partitions;

	// indices of fields to be returned, with projection applied (if any)
	private int[] selectedFields;

	//We should limit the input read count of this splits, -1 represents no limit.
	private long limit;

	private transient long currentReadCount = 0L;

	@VisibleForTesting
	protected transient SplitReader reader;

	private boolean useMapRedReader;

	public HiveTableInputFormat(
			JobConf jobConf,
			CatalogTable catalogTable,
			List<HiveTablePartition> partitions,
			int[] projectedFields,
			long limit,
			String hiveVersion,
			boolean useMapRedReader) {
		super(jobConf.getCredentials());
		this.partitionKeys = catalogTable.getPartitionKeys();
		this.fieldTypes = catalogTable.getSchema().getFieldDataTypes();
		this.fieldNames = catalogTable.getSchema().getFieldNames();
		this.limit = limit;
		this.hiveVersion = hiveVersion;
		checkNotNull(catalogTable, "catalogTable can not be null.");
		this.partitions = checkNotNull(partitions, "partitions can not be null.");
		this.jobConf = new JobConf(jobConf);
		int rowArity = catalogTable.getSchema().getFieldCount();
		selectedFields = projectedFields != null ? projectedFields : IntStream.range(0, rowArity).toArray();
		this.useMapRedReader = useMapRedReader;
	}

	@Override
	public void configure(org.apache.flink.configuration.Configuration parameters) {
	}

	@Override
	public void open(HiveTableInputSplit split) throws IOException {
		if (!useMapRedReader && useOrcVectorizedRead(split.getHiveTablePartition())) {
			this.reader = new HiveVectorizedOrcSplitReader(
					hiveVersion, jobConf, fieldNames, fieldTypes, selectedFields, split);
		} else {
			this.reader = new HiveMapredSplitReader(jobConf, partitionKeys, fieldTypes, selectedFields, split,
					HiveShimLoader.loadHiveShim(hiveVersion));
		}
		currentReadCount = 0L;
	}

	private boolean useOrcVectorizedRead(HiveTablePartition partition) {
		boolean isOrc = partition.getStorageDescriptor().getSerdeInfo().getSerializationLib()
				.toLowerCase().contains("orc");
		if (!isOrc) {
			return false;
		}

		if (hiveVersion.startsWith("1.")) {
			LOG.info("Fallback to hadoop mapred reader, unsupported hive version: " + hiveVersion);
			return false;
		}

		for (int i : selectedFields) {
			switch (fieldTypes[i].getLogicalType().getTypeRoot()) {
				case CHAR:
				case VARCHAR:
				case BOOLEAN:
				case BINARY:
				case VARBINARY:
				case DECIMAL:
				case TINYINT:
				case SMALLINT:
				case INTEGER:
				case BIGINT:
				case FLOAT:
				case DOUBLE:
				case DATE:
				case TIME_WITHOUT_TIME_ZONE:
				case TIMESTAMP_WITHOUT_TIME_ZONE:
				case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
					break;
				case TIMESTAMP_WITH_TIME_ZONE:
				case INTERVAL_YEAR_MONTH:
				case INTERVAL_DAY_TIME:
				case ARRAY:
				case MULTISET:
				case MAP:
				case ROW:
				case DISTINCT_TYPE:
				case STRUCTURED_TYPE:
				case NULL:
				case RAW:
				case SYMBOL:
				default:
					LOG.info("Fallback to hadoop mapred reader, unsupported field type: " + fieldTypes[i]);
					return false;
			}
		}

		LOG.info("Use flink orc ColumnarRow reader.");
		return true;
	}

	@Override
	public boolean reachedEnd() throws IOException {
		if (limit > 0 && currentReadCount >= limit) {
			return true;
		} else {
			return reader.reachedEnd();
		}
	}

	@Override
	public BaseRow nextRecord(BaseRow reuse) throws IOException {
		currentReadCount++;
		return reader.nextRecord(reuse);
	}

	@Override
	public void close() throws IOException {
		if (this.reader != null) {
			this.reader.close();
			this.reader = null;
		}
	}

	@Override
	public HiveTableInputSplit[] createInputSplits(int minNumSplits)
			throws IOException {
		List<HiveTableInputSplit> hiveSplits = new ArrayList<>();
		int splitNum = 0;
		for (HiveTablePartition partition : partitions) {
			StorageDescriptor sd = partition.getStorageDescriptor();
			InputFormat format;
			try {
				format = (InputFormat)
					Class.forName(sd.getInputFormat(), true, Thread.currentThread().getContextClassLoader()).newInstance();
			} catch (Exception e) {
				throw new FlinkHiveException("Unable to instantiate the hadoop input format", e);
			}
			ReflectionUtils.setConf(format, jobConf);
			jobConf.set(INPUT_DIR, sd.getLocation());
			//TODO: we should consider how to calculate the splits according to minNumSplits in the future.
			org.apache.hadoop.mapred.InputSplit[] splitArray = format.getSplits(jobConf, minNumSplits);
			for (org.apache.hadoop.mapred.InputSplit inputSplit : splitArray) {
				hiveSplits.add(new HiveTableInputSplit(splitNum++, inputSplit, jobConf, partition));
			}
		}

		return hiveSplits.toArray(new HiveTableInputSplit[0]);
	}

	@Override
	public BaseStatistics getStatistics(BaseStatistics cachedStats) {
		// no statistics available
		return null;
	}

	@Override
	public InputSplitAssigner getInputSplitAssigner(HiveTableInputSplit[] inputSplits) {
		return new LocatableInputSplitAssigner(inputSplits);
	}

	// --------------------------------------------------------------------------------------------
	//  Custom serialization methods
	// --------------------------------------------------------------------------------------------

	private void writeObject(ObjectOutputStream out) throws IOException {
		super.write(out);
		jobConf.write(out);
		out.writeObject(partitionKeys);
		out.writeObject(fieldTypes);
		out.writeObject(fieldNames);
		out.writeObject(partitions);
		out.writeObject(selectedFields);
		out.writeObject(limit);
		out.writeObject(hiveVersion);
		out.writeBoolean(useMapRedReader);
	}

	@SuppressWarnings("unchecked")
	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
		super.read(in);
		if (jobConf == null) {
			jobConf = new JobConf();
		}
		jobConf.readFields(in);
		jobConf.getCredentials().addAll(this.credentials);
		Credentials currentUserCreds = getCredentialsFromUGI(UserGroupInformation.getCurrentUser());
		if (currentUserCreds != null) {
			jobConf.getCredentials().addAll(currentUserCreds);
		}
		partitionKeys = (List<String>) in.readObject();
		fieldTypes = (DataType[]) in.readObject();
		fieldNames = (String[]) in.readObject();
		partitions = (List<HiveTablePartition>) in.readObject();
		selectedFields = (int[]) in.readObject();
		limit = (long) in.readObject();
		hiveVersion = (String) in.readObject();
		useMapRedReader = in.readBoolean();
	}
}
