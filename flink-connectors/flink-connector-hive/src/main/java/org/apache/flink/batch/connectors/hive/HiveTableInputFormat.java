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

import org.apache.flink.api.common.io.LocatableInputSplitAssigner;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.hadoop.common.HadoopInputFormatCommonBase;
import org.apache.flink.api.java.hadoop.mapred.wrapper.HadoopDummyReporter;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.table.catalog.hive.util.HiveTableUtil;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.GenericRow;
import org.apache.flink.table.typeutils.BaseRowTypeInfo;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
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

import static org.apache.hadoop.mapreduce.lib.input.FileInputFormat.INPUT_DIR;

/**
 * The HiveTableInputFormat are inspired by the HCatInputFormat and HadoopInputFormatBase.
 */
public class HiveTableInputFormat extends HadoopInputFormatCommonBase<BaseRow, HiveTableInputSplit>
		implements ResultTypeQueryable {
	private static final long serialVersionUID = 6351448428766433164L;
	private static Logger logger = LoggerFactory.getLogger(HiveTableInputFormat.class);

	private JobConf jobConf;

	protected transient Writable key;
	protected transient Writable value;

	private transient RecordReader<Writable, Writable> recordReader;
	protected transient boolean fetched = false;
	protected transient boolean hasNext;

	private Boolean isPartitioned;
	private BaseRowTypeInfo baseRowTypeInfo;

	// Necessary info to init deserializer
	private String[] partitionColNames;
	private List<HiveTablePartition> partitions;
	private transient Deserializer deserializer;
	private transient List<? extends StructField> fieldRefs;
	private transient StructObjectInspector oi;
	private transient InputFormat mapredInputFormat;
	private transient HiveTablePartition hiveTablePartition;

	public HiveTableInputFormat(
			JobConf jobConf,
			Boolean isPartitioned,
			String[] partitionColNames,
			List<HiveTablePartition> partitions,
			BaseRowTypeInfo baseRowTypeInfo) {
		super(jobConf.getCredentials());
		this.baseRowTypeInfo = baseRowTypeInfo;
		this.jobConf = new JobConf(jobConf);
		this.jobConf = jobConf;
		this.isPartitioned = isPartitioned;
		this.partitionColNames = partitionColNames;
		this.partitions = partitions;
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
			throw new RuntimeException("Unable to instantiate the hadoop input format", e);
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
			Properties properties = HiveTableUtil.createPropertiesFromStorageDescriptor(sd);
			SerDeUtils.initializeSerDe(deserializer, conf, properties, null);
			// Get the row structure
			oi = (StructObjectInspector) deserializer.getObjectInspector();
			fieldRefs = oi.getAllStructFieldRefs();
		} catch (Exception e) {
			logger.error("Error happens when deserialize from storage file.");
			throw new RuntimeException(e);
		}
	}

	@Override
	public HiveTableInputSplit[] createInputSplits(int minNumSplits)
			throws IOException {
		List<HiveTableInputSplit> hiSplit = new ArrayList<>();
		int splitNum = 0;
		for (HiveTablePartition partition : partitions) {
			StorageDescriptor sd = partition.getStorageDescriptor();
			InputFormat format;
			try {
				format = (InputFormat)
					Class.forName(sd.getInputFormat(), true, Thread.currentThread().getContextClassLoader()).newInstance();
			} catch (Exception e) {
				throw new RuntimeException("Unable to instantiate the hadoop input format", e);
			}
			ReflectionUtils.setConf(format, jobConf);
			jobConf.set(INPUT_DIR, sd.getLocation());
			//TODO: we should consider how to calculate the splits according to minNumSplits in the future.
			org.apache.hadoop.mapred.InputSplit[] splitArray = format.getSplits(jobConf, minNumSplits);
			for (int i = 0; i < splitArray.length; i++) {
				hiSplit.add(new HiveTableInputSplit(splitNum++, splitArray[i], jobConf, partition));
			}
		}

		return hiSplit.toArray(new HiveTableInputSplit[hiSplit.size()]);
	}

	@Override
	public void configure(org.apache.flink.configuration.Configuration parameters) {

	}

	// Todo: refactor code to get statistics from hms or partition.
	@Override
	public BaseStatistics getStatistics(BaseStatistics cachedStats) throws IOException {
		// only gather base statistics for FileInputFormats
		if (!(mapredInputFormat instanceof FileInputFormat)) {
			return null;
		}

		final org.apache.flink.api.common.io.FileInputFormat.FileBaseStatistics cachedFileStats =
				(cachedStats instanceof org.apache.flink.api.common.io.FileInputFormat.FileBaseStatistics) ?
				(org.apache.flink.api.common.io.FileInputFormat.FileBaseStatistics) cachedStats : null;

		try {
			final org.apache.hadoop.fs.Path[] paths = FileInputFormat.getInputPaths(this.jobConf);

			return HiveTableUtil.getFileStats(cachedFileStats, paths, new ArrayList<FileStatus>(1));
		} catch (IOException ioex) {
			if (logger.isWarnEnabled()) {
				logger.warn("Could not determine statistics due to an io error: "
							+ ioex.getMessage());
			}
		} catch (Throwable t) {
			if (logger.isErrorEnabled()) {
				logger.error("Unexpected problem while getting the file statistics: " + t.getMessage(), t);
			}
		}

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
		}
	}

	protected void fetchNext() throws IOException {
		hasNext = this.recordReader.next(key, value);
		fetched = true;
	}

	@Override
	public BaseRow nextRecord(BaseRow ignore) throws IOException {
		if (!this.fetched) {
			fetchNext();
		}
		if (!this.hasNext) {
			return null;
		}
		Object[] values = new Object[baseRowTypeInfo.getArity()];
		try {
			Object o = deserializer.deserialize(value);
			int index = 0;
			for (; index < fieldRefs.size(); index++) {
				StructField fref = fieldRefs.get(index);
				values[index] = HiveRecordSerDe.serializeField(oi.getStructFieldData(o, fref), fref.getFieldObjectInspector());
			}
			if (isPartitioned) {
				for (String partition : partitionColNames){
					values[index++] = hiveTablePartition.getPartitionValues().get(partition);
				}
			}
		} catch (Exception e){
			logger.error("Error happens when converting hive data type to flink data type.");
			throw new RuntimeException(e);
		}
		this.fetched = false;
		return GenericRow.of(values);
	}

	@Override
	public TypeInformation getProducedType() {
		return baseRowTypeInfo;
	}

	// --------------------------------------------------------------------------------------------
	//  Custom serialization methods
	// --------------------------------------------------------------------------------------------

	private void writeObject(ObjectOutputStream out) throws IOException {
		super.write(out);
		jobConf.write(out);
		out.writeObject(isPartitioned);
		out.writeObject(baseRowTypeInfo);

		out.writeObject(partitionColNames);
		out.writeObject(partitions);
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
		isPartitioned = (Boolean) in.readObject();
		baseRowTypeInfo = (BaseRowTypeInfo) in.readObject();

		partitionColNames = (String[]) in.readObject();
		partitions = (List<HiveTablePartition>) in.readObject();
	}

	/**
	 * Use this class to build HiveTableInputFormat.
	 */
	public static class Builder {
		private BaseRowTypeInfo baseRowTypeInfo;
		private JobConf jobConf;
		private Boolean isPartitioned;
		private String[] partitionColNames;
		private List<HiveTablePartition> partitions;

		public Builder(
				BaseRowTypeInfo baseRowTypeInfo, JobConf jobConf, String dbName, String tableName, Boolean
				isPartitioned, String[] partitionColNames, List<HiveTablePartition> partitions) {
			this.baseRowTypeInfo = baseRowTypeInfo;
			this.jobConf = jobConf;
			this.isPartitioned = isPartitioned;
			this.partitionColNames = partitionColNames;
			this.partitions = partitions;
		}

		public HiveTableInputFormat build() {
			try {
				return new HiveTableInputFormat(jobConf, isPartitioned, partitionColNames, partitions, baseRowTypeInfo);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	}
}
