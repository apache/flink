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

import org.apache.flink.api.common.io.LocatableInputSplitAssigner;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.java.hadoop.common.HadoopInputFormatCommonBase;
import org.apache.flink.api.java.hadoop.mapred.wrapper.HadoopDummyReporter;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.hive.util.HiveTableUtil;
import org.apache.flink.table.functions.hive.conversion.HiveInspectors;
import org.apache.flink.types.Row;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
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
import java.util.Properties;
import java.util.stream.IntStream;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.hadoop.mapreduce.lib.input.FileInputFormat.INPUT_DIR;

/**
 * The HiveTableInputFormat are inspired by the HCatInputFormat and HadoopInputFormatBase.
 * It's used to read from hive partition/non-partition table.
 */
public class HiveTableInputFormat extends HadoopInputFormatCommonBase<Row, HiveTableInputSplit> {
	private static final long serialVersionUID = 6351448428766433164L;
	private static Logger logger = LoggerFactory.getLogger(HiveTableInputFormat.class);

	private JobConf jobConf;

	protected transient Writable key;
	protected transient Writable value;

	private transient RecordReader<Writable, Writable> recordReader;
	protected transient boolean fetched = false;
	protected transient boolean hasNext;

	//Necessary info to init deserializer
	private List<String> partitionColNames;
	//For non-partition hive table, partitions only contains one partition which partitionValues is empty.
	private List<HiveTablePartition> partitions;
	private transient Deserializer deserializer;
	//Hive StructField list contain all related info for specific serde.
	private transient List<? extends StructField> structFields;
	//StructObjectInspector in hive helps us to look into the internal structure of a struct object.
	private transient StructObjectInspector structObjectInspector;
	private transient InputFormat mapredInputFormat;
	private transient HiveTablePartition hiveTablePartition;

	// indices of fields to be returned, with projection applied (if any)
	// TODO: push projection into underlying input format that supports it
	private int[] fields;
	// Remember whether a row instance is reused. No need to set partition fields for reused rows
	private transient boolean rowReused;

	public HiveTableInputFormat(
			JobConf jobConf,
			CatalogTable catalogTable,
			List<HiveTablePartition> partitions,
			int[] projectedFields) {
		super(jobConf.getCredentials());
		checkNotNull(catalogTable, "catalogTable can not be null.");
		this.partitions = checkNotNull(partitions, "partitions can not be null.");

		this.jobConf = new JobConf(jobConf);
		this.partitionColNames = catalogTable.getPartitionKeys();
		int rowArity = catalogTable.getSchema().getFieldCount();
		fields = projectedFields != null ? projectedFields : IntStream.range(0, rowArity).toArray();
	}

	@Override
	public void open(HiveTableInputSplit split) throws IOException {
		this.hiveTablePartition = split.getHiveTablePartition();
		StorageDescriptor sd = hiveTablePartition.getStorageDescriptor();
		jobConf.set(INPUT_DIR, sd.getLocation());
		try {
			this.mapredInputFormat = (InputFormat)
				Class.forName(sd.getInputFormat(), true, Thread.currentThread().getContextClassLoader()).newInstance();
		} catch (Exception e) {
			throw new FlinkHiveException("Unable to instantiate the hadoop input format", e);
		}
		ReflectionUtils.setConf(mapredInputFormat, jobConf);
		if (this.mapredInputFormat instanceof Configurable) {
			((Configurable) this.mapredInputFormat).setConf(this.jobConf);
		} else if (this.mapredInputFormat instanceof JobConfigurable) {
			((JobConfigurable) this.mapredInputFormat).configure(this.jobConf);
		}
		this.recordReader = this.mapredInputFormat.getRecordReader(split.getHadoopInputSplit(),
			jobConf, new HadoopDummyReporter());
		if (this.recordReader instanceof Configurable) {
			((Configurable) this.recordReader).setConf(jobConf);
		}
		key = this.recordReader.createKey();
		value = this.recordReader.createValue();
		this.fetched = false;
		try {
			deserializer = (Deserializer) Class.forName(sd.getSerdeInfo().getSerializationLib()).newInstance();
			Configuration conf = new Configuration();
			//properties are used to initialize hive Deserializer properly.
			Properties properties = HiveTableUtil.createPropertiesFromStorageDescriptor(sd);
			SerDeUtils.initializeSerDe(deserializer, conf, properties, null);
			structObjectInspector = (StructObjectInspector) deserializer.getObjectInspector();
			structFields = structObjectInspector.getAllStructFieldRefs();
		} catch (Exception e) {
			throw new FlinkHiveException("Error happens when deserialize from storage file.", e);
		}
		rowReused = false;
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
			for (int i = 0; i < splitArray.length; i++) {
				hiveSplits.add(new HiveTableInputSplit(splitNum++, splitArray[i], jobConf, partition));
			}
		}

		return hiveSplits.toArray(new HiveTableInputSplit[hiveSplits.size()]);
	}

	@Override
	public void configure(org.apache.flink.configuration.Configuration parameters) {

	}

	@Override
	public BaseStatistics getStatistics(BaseStatistics cachedStats) throws IOException {
		// no statistics available
		return null;
	}

	@Override
	public InputSplitAssigner getInputSplitAssigner(HiveTableInputSplit[] inputSplits) {
		return new LocatableInputSplitAssigner(inputSplits);
	}

	@Override
	public boolean reachedEnd() throws IOException {
		if (!fetched) {
			fetchNext();
		}
		return !hasNext;
	}

	@Override
	public void close() throws IOException {
		if (this.recordReader != null) {
			this.recordReader.close();
			this.recordReader = null;
		}
	}

	protected void fetchNext() throws IOException {
		hasNext = this.recordReader.next(key, value);
		fetched = true;
	}

	@Override
	public Row nextRecord(Row reuse) throws IOException {
		if (reachedEnd()) {
			return null;
		}
		try {
			//Use HiveDeserializer to deserialize an object out of a Writable blob
			Object hiveRowStruct = deserializer.deserialize(value);
			for (int i = 0; i < fields.length; i++) {
				// set non-partition columns
				if (fields[i] < structFields.size()) {
					StructField structField = structFields.get(fields[i]);
					Object object = HiveInspectors.toFlinkObject(structField.getFieldObjectInspector(),
							structObjectInspector.getStructFieldData(hiveRowStruct, structField));
					reuse.setField(i, object);
				}
			}
		} catch (Exception e) {
			logger.error("Error happens when converting hive data type to flink data type.");
			throw new FlinkHiveException(e);
		}
		if (!rowReused) {
			// set partition columns
			if (!partitionColNames.isEmpty()) {
				for (int i = 0; i < fields.length; i++) {
					if (fields[i] >= structFields.size()) {
						String partition = partitionColNames.get(fields[i] - structFields.size());
						reuse.setField(i, hiveTablePartition.getPartitionSpec().get(partition));
					}
				}
			}
			rowReused = true;
		}
		this.fetched = false;
		return reuse;
	}

	// --------------------------------------------------------------------------------------------
	//  Custom serialization methods
	// --------------------------------------------------------------------------------------------

	private void writeObject(ObjectOutputStream out) throws IOException {
		super.write(out);
		jobConf.write(out);
		out.writeObject(partitionColNames);
		out.writeObject(partitions);
		out.writeObject(fields);
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
		partitionColNames = (List<String>) in.readObject();
		partitions = (List<HiveTablePartition>) in.readObject();
		fields = (int[]) in.readObject();
	}
}
