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

import org.apache.flink.api.common.io.FinalizeOnMaster;
import org.apache.flink.api.common.io.InitializeOnMaster;
import org.apache.flink.api.java.hadoop.common.HadoopOutputFormatCommonBase;
import org.apache.flink.api.java.hadoop.mapreduce.utils.HadoopUtils;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.hive.HMSClientFactory;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.DataFormatConverters;
import org.apache.flink.util.StringUtils;

import org.apache.flink.shaded.guava18.com.google.common.base.Preconditions;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.io.HiveFileFormatUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.JobContextImpl;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.OutputCommitter;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TaskAttemptContext;
import org.apache.hadoop.mapred.TaskAttemptContextImpl;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.flink.api.java.hadoop.common.HadoopInputFormatCommonBase.getCredentialsFromUGI;
import static org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.OUTDIR;

/**
 * HiveTableOutputFormat used to write data to hive table, including non-partition and partitioned table.
 */
public class HiveTableOutputFormat extends HadoopOutputFormatCommonBase<BaseRow> implements InitializeOnMaster,
	FinalizeOnMaster {

	private static final Logger LOG = LoggerFactory.getLogger(HiveTableOutputFormat.class);

	private static final long serialVersionUID = 1L;

	private transient JobConf jobConf;
	private transient String dbName;
	private transient String tableName;
	private transient List<String> partitionCols;
	private transient RowTypeInfo rowTypeInfo;
	private transient HiveTablePartition hiveTablePartition;
	private transient Properties tblProperties;
	private transient boolean overwrite;
	private transient boolean isPartitioned;
	private transient boolean isDynamicPartition;

	private transient AbstractSerDe serializer;
	//StructObjectInspector represents the hive row structure.
	private transient StructObjectInspector sois;
	private transient Class<? extends Writable> outputClass;
	private transient TaskAttemptContext context;
	private transient DataFormatConverters.DataFormatConverter[] converters;

	// Maps a partition dir name to the corresponding writer. Used for dynamic partitioning.
	private transient Map<String, HivePartitionWriter> partitionToWriter;
	// Writer for non-partitioned and static partitioned table
	private transient HivePartitionWriter staticWriter;

	public HiveTableOutputFormat(JobConf jobConf, String dbName, String tableName, List<String> partitionCols,
								RowTypeInfo rowTypeInfo, HiveTablePartition hiveTablePartition,
								Properties tblProperties, boolean overwrite) {
		super(jobConf.getCredentials());
		HadoopUtils.mergeHadoopConf(jobConf);
		this.jobConf = jobConf;
		this.dbName = dbName;
		this.tableName = tableName;
		this.partitionCols = partitionCols;
		this.rowTypeInfo = rowTypeInfo;
		this.hiveTablePartition = hiveTablePartition;
		this.tblProperties = tblProperties;
		this.overwrite = overwrite;
		isPartitioned = partitionCols != null && !partitionCols.isEmpty();
		isDynamicPartition = isPartitioned && partitionCols.size() > hiveTablePartition.getPartitionSpec().size();
	}

	//  Custom serialization methods

	private void writeObject(ObjectOutputStream out) throws IOException {
		super.write(out);
		jobConf.write(out);
		out.writeObject(isPartitioned);
		out.writeObject(isDynamicPartition);
		out.writeObject(overwrite);
		out.writeObject(rowTypeInfo);
		out.writeObject(hiveTablePartition);
		out.writeObject(partitionCols);
		out.writeObject(dbName);
		out.writeObject(tableName);
		out.writeObject(tblProperties);
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
		isPartitioned = (boolean) in.readObject();
		isDynamicPartition = (boolean) in.readObject();
		overwrite = (boolean) in.readObject();
		rowTypeInfo = (RowTypeInfo) in.readObject();
		hiveTablePartition = (HiveTablePartition) in.readObject();
		partitionCols = (List<String>) in.readObject();
		dbName = (String) in.readObject();
		tableName = (String) in.readObject();
		partitionToWriter = new HashMap<>();
		tblProperties = (Properties) in.readObject();
	}

	@Override
	public void finalizeGlobal(int parallelism) throws IOException {
		StorageDescriptor jobSD = hiveTablePartition.getStorageDescriptor();
		Path stagingDir = new Path(jobSD.getLocation());
		FileSystem fs = stagingDir.getFileSystem(jobConf);
		IMetaStoreClient client = null;
		try {
			client = HMSClientFactory.create(new HiveConf(jobConf, HiveConf.class));
			Table table = client.getTable(dbName, tableName);
			if (!isDynamicPartition) {
				commitJob(stagingDir.toString());
			}
			if (isPartitioned) {
				// TODO: to be implemented
			} else {
				moveFiles(stagingDir, new Path(table.getSd().getLocation()));
			}
		} catch (TException e) {
			throw new CatalogException("Failed to query Hive metaStore", e);
		} finally {
			if (client != null) {
				client.close();
			}
			fs.delete(stagingDir, true);
		}
	}

	@Override
	public void initializeGlobal(int parallelism) throws IOException {
	}

	@Override
	public void configure(Configuration parameters) {
		// since our writers are transient, we don't need to do anything here
	}

	@Override
	public void open(int taskNumber, int numTasks) throws IOException {
		try {
			StorageDescriptor sd = hiveTablePartition.getStorageDescriptor();
			serializer = (AbstractSerDe) Class.forName(sd.getSerdeInfo().getSerializationLib()).newInstance();
			ReflectionUtils.setConf(serializer, jobConf);
			SerDeUtils.initializeSerDe(serializer, jobConf, tblProperties, null);
			outputClass = serializer.getSerializedClass();
		} catch (IllegalAccessException | SerDeException | InstantiationException | ClassNotFoundException e) {
			LOG.error("Error initializing Hive serializer");
			throw new RuntimeException(e);
		}
		if (Integer.toString(taskNumber).length() > 6) {
			throw new IOException("Task id too large.");
		}

		TaskAttemptID taskAttemptID = TaskAttemptID.forName("attempt__0000_r_"
			+ String.format("%" + (6 - Integer.toString(taskNumber).length()) + "s", " ").replace(" ", "0")
			+ taskNumber + "_0");

		this.jobConf.set("mapred.task.id", taskAttemptID.toString());
		this.jobConf.setInt("mapred.task.partition", taskNumber);
		// for hadoop 2.2
		this.jobConf.set("mapreduce.task.attempt.id", taskAttemptID.toString());
		this.jobConf.setInt("mapreduce.task.partition", taskNumber);

		this.context = new TaskAttemptContextImpl(this.jobConf, taskAttemptID);

		if (!isDynamicPartition) {
			staticWriter = writerForLocation(hiveTablePartition.getStorageDescriptor().getLocation());
		}

		List<ObjectInspector> objectInspectors = new ArrayList<>();
		for (int i = 0; i < rowTypeInfo.getArity() - partitionCols.size(); i++) {
			objectInspectors.add(HiveTableUtil.getObjectInspector(rowTypeInfo.getTypeAt(i)));
		}
		converters = new DataFormatConverters.DataFormatConverter[rowTypeInfo.getArity()];

		if (!isPartitioned) {
			sois = ObjectInspectorFactory.getStandardStructObjectInspector(
				Arrays.asList(rowTypeInfo.getFieldNames()),
				objectInspectors);
		} else {
			sois = ObjectInspectorFactory.getStandardStructObjectInspector(
				Arrays.asList(rowTypeInfo.getFieldNames()).subList(0, rowTypeInfo.getArity() - partitionCols.size()),
				objectInspectors);
		}
	}

	@Override
	public void writeRecord(BaseRow record) throws IOException {
		try {
			HivePartitionWriter partitionWriter = staticWriter;
			if (isDynamicPartition) {
				// TODO: to be implemented
			}
			partitionWriter.recordWriter.write(serializer.serialize(getConvertedRow(record), sois));
		} catch (IOException | SerDeException e) {
			throw new IOException("Could not write Record.", e);
		}
	}

	// moves all files under srcDir into destDir
	private void moveFiles(Path srcDir, Path destDir) throws IOException {
		if (!srcDir.equals(destDir)) {
			// TODO: src and dest may be on different FS
			FileSystem fs = destDir.getFileSystem(jobConf);
			Preconditions.checkState(fs.exists(destDir) || fs.mkdirs(destDir), "Failed to create dest path " + destDir);
			if (overwrite) {
				// delete existing files for overwrite
				// TODO: support setting auto-purge?
				final boolean purge = true;
				// Note we assume the srcDir is a hidden dir, otherwise it will be deleted if it's a sub-dir of destDir
				FileStatus[] existingFiles = fs.listStatus(destDir, FileUtils.HIDDEN_FILES_PATH_FILTER);
				if (existingFiles != null) {
					for (FileStatus existingFile : existingFiles) {
						Preconditions.checkState(FileUtils.moveToTrash(fs, existingFile.getPath(), jobConf, purge),
							"Failed to overwrite existing file " + existingFile);
					}
				}
			}
			FileStatus[] srcFiles = fs.listStatus(srcDir, FileUtils.HIDDEN_FILES_PATH_FILTER);
			for (FileStatus srcFile : srcFiles) {
				Path srcPath = srcFile.getPath();
				Path destPath = new Path(destDir, srcPath.getName());
				int count = 1;
				while (!fs.rename(srcPath, destPath)) {
					String name = srcPath.getName() + "_copy_" + count;
					destPath = new Path(destDir, name);
					count++;
				}
			}
		}
	}

	private void commitJob(String location) throws IOException {
		jobConf.set(OUTDIR, location);
		JobContext jobContext = new JobContextImpl(this.jobConf, new JobID());
		OutputCommitter outputCommitter = this.jobConf.getOutputCommitter();
		// finalize HDFS output format
		outputCommitter.commitJob(jobContext);
	}

	// converts each field in the record to external object
	private Object getConvertedRow(BaseRow record) {
		int actualArity = isPartitioned ? record.getArity() - partitionCols.size() : record.getArity();
		List<Object> res = new ArrayList<>(actualArity);
		for (int i = 0; i < actualArity; i++) {
			if (converters[i] == null) {
				converters[i] = DataFormatConverters.getConverterForTypeInfo(rowTypeInfo.getTypeAt(i));
			}
			res.add(converters[i].toExternal(record, i));
		}
		return res;
	}

	@Override
	public void close() throws IOException {
		for (HivePartitionWriter partitionWriter : getPartitionWriters()) {
			// TODO: need a way to decide whether to abort
			partitionWriter.recordWriter.close(false);
			if (partitionWriter.outputCommitter.needsTaskCommit(context)) {
				partitionWriter.outputCommitter.commitTask(context);
			}
		}
	}

	// get all partition writers we've created
	private List<HivePartitionWriter> getPartitionWriters() {
		if (isDynamicPartition) {
			return new ArrayList<>(partitionToWriter.values());
		} else {
			return Collections.singletonList(staticWriter);
		}
	}

	private HivePartitionWriter writerForLocation(String location) throws IOException {
		JobConf clonedConf = new JobConf(jobConf);
		clonedConf.set(OUTDIR, location);
		OutputFormat outputFormat;
		try {
			StorageDescriptor sd = hiveTablePartition.getStorageDescriptor();
			Class outputFormatClz = Class.forName(sd.getOutputFormat(), true,
				Thread.currentThread().getContextClassLoader());
			outputFormatClz = HiveFileFormatUtils.getOutputFormatSubstitute(outputFormatClz);
			outputFormat = (OutputFormat) outputFormatClz.newInstance();
		} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
			throw new RuntimeException("Unable to instantiate the hadoop output format", e);
		}
		ReflectionUtils.setConf(outputFormat, clonedConf);
		OutputCommitter outputCommitter = clonedConf.getOutputCommitter();
		JobContext jobContext = new JobContextImpl(clonedConf, new JobID());
		outputCommitter.setupJob(jobContext);
		final boolean isCompressed = clonedConf.getBoolean(HiveConf.ConfVars.COMPRESSRESULT.varname, false);
		if (isCompressed) {
			String codecStr = clonedConf.get(HiveConf.ConfVars.COMPRESSINTERMEDIATECODEC.varname);
			if (!StringUtils.isNullOrWhitespaceOnly(codecStr)) {
				try {
					Class<? extends CompressionCodec> codec =
						(Class<? extends CompressionCodec>) Class.forName(codecStr, true,
							Thread.currentThread().getContextClassLoader());
					FileOutputFormat.setOutputCompressorClass(clonedConf, codec);
				} catch (ClassNotFoundException e) {
					throw new RuntimeException(e);
				}
			}
			String typeStr = clonedConf.get(HiveConf.ConfVars.COMPRESSINTERMEDIATETYPE.varname);
			if (!StringUtils.isNullOrWhitespaceOnly(typeStr)) {
				SequenceFile.CompressionType style = SequenceFile.CompressionType.valueOf(typeStr);
				SequenceFileOutputFormat.setOutputCompressionType(clonedConf, style);
			}
		}
		String taskPartition = String.valueOf(clonedConf.getInt("mapreduce.task.partition", -1));
		Path taskPath = FileOutputFormat.getTaskOutputPath(clonedConf, taskPartition);
		FileSinkOperator.RecordWriter recordWriter;
		try {
			recordWriter = HiveFileFormatUtils.getRecordWriter(clonedConf, outputFormat,
				outputClass, isCompressed, tblProperties, taskPath, Reporter.NULL);
		} catch (HiveException e) {
			throw new IOException(e);
		}
		return new HivePartitionWriter(clonedConf, outputFormat, recordWriter, outputCommitter);
	}

	private static class HivePartitionWriter {
		private final JobConf jobConf;
		private final OutputFormat outputFormat;
		private final FileSinkOperator.RecordWriter recordWriter;
		private final OutputCommitter outputCommitter;

		HivePartitionWriter(JobConf jobConf, OutputFormat outputFormat, FileSinkOperator.RecordWriter recordWriter,
							OutputCommitter outputCommitter) {
			this.jobConf = jobConf;
			this.outputFormat = outputFormat;
			this.recordWriter = recordWriter;
			this.outputCommitter = outputCommitter;
		}
	}
}
