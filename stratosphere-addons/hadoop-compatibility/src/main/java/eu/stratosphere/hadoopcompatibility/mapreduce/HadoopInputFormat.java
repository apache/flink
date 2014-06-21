/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.hadoopcompatibility.mapreduce;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import eu.stratosphere.api.common.io.FileInputFormat.FileBaseStatistics;
import eu.stratosphere.api.common.io.InputFormat;
import eu.stratosphere.api.common.io.statistics.BaseStatistics;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.typeutils.ResultTypeQueryable;
import eu.stratosphere.api.java.typeutils.TupleTypeInfo;
import eu.stratosphere.api.java.typeutils.WritableTypeInfo;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.core.fs.FileStatus;
import eu.stratosphere.core.fs.FileSystem;
import eu.stratosphere.core.fs.Path;
import eu.stratosphere.hadoopcompatibility.mapreduce.utils.HadoopUtils;
import eu.stratosphere.hadoopcompatibility.mapreduce.wrapper.HadoopInputSplit;
import eu.stratosphere.types.TypeInformation;

public class HadoopInputFormat<K extends Writable, V extends Writable> implements InputFormat<Tuple2<K,V>, HadoopInputSplit>, ResultTypeQueryable<Tuple2<K,V>> {
	
	private static final long serialVersionUID = 1L;
	
	private static final Log LOG = LogFactory.getLog(HadoopInputFormat.class);
	
	private org.apache.hadoop.mapreduce.InputFormat<K, V> mapreduceInputFormat;
	private Class<K> keyClass;
	private Class<V> valueClass;
	private org.apache.hadoop.conf.Configuration configuration;
	
	private transient RecordReader<K, V> recordReader;
	private boolean fetched = false;
	private boolean hasNext;
	
	public HadoopInputFormat() {
		super();
	}
	
	public HadoopInputFormat(org.apache.hadoop.mapreduce.InputFormat<K,V> mapreduceInputFormat, Class<K> key, Class<V> value, Job job) {
		super();
		this.mapreduceInputFormat = mapreduceInputFormat;
		this.keyClass = key;
		this.valueClass = value;
		this.configuration = job.getConfiguration();
		HadoopUtils.mergeHadoopConf(configuration);
	}
	
	public void setConfiguration(org.apache.hadoop.conf.Configuration configuration) {
		this.configuration = configuration;
	}
	
	public org.apache.hadoop.mapreduce.InputFormat<K,V> getHadoopInputFormat() {
		return this.mapreduceInputFormat;
	}
	
	public void setHadoopInputFormat(org.apache.hadoop.mapreduce.InputFormat<K,V> mapreduceInputFormat) {
		this.mapreduceInputFormat = mapreduceInputFormat;
	}
	
	public org.apache.hadoop.conf.Configuration getConfiguration() {
		return this.configuration;
	}
	
	// --------------------------------------------------------------------------------------------
	//  InputFormat
	// --------------------------------------------------------------------------------------------
	
	@Override
	public void configure(Configuration parameters) {
		// nothing to do
	}
	
	@Override
	public BaseStatistics getStatistics(BaseStatistics cachedStats) throws IOException {
		// only gather base statistics for FileInputFormats
		if(!(mapreduceInputFormat instanceof FileInputFormat)) {
			return null;
		}
		
		JobContext jobContext = null;
		try {
			jobContext = HadoopUtils.instantiateJobContext(configuration, null);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		
		final FileBaseStatistics cachedFileStats = (cachedStats != null && cachedStats instanceof FileBaseStatistics) ?
				(FileBaseStatistics) cachedStats : null;
				
				try {
					final org.apache.hadoop.fs.Path[] paths = FileInputFormat.getInputPaths(jobContext);
					return getFileStats(cachedFileStats, paths, new ArrayList<FileStatus>(1));
				} catch (IOException ioex) {
					if (LOG.isWarnEnabled()) {
						LOG.warn("Could not determine statistics due to an io error: "
								+ ioex.getMessage());
					}
				} catch (Throwable t) {
					if (LOG.isErrorEnabled()) {
						LOG.error("Unexpected problem while getting the file statistics: "
								+ t.getMessage(), t);
					}
				}
				
				// no statistics available
				return null;
	}
	
	@Override
	public HadoopInputSplit[] createInputSplits(int minNumSplits)
			throws IOException {
		configuration.setInt("mapreduce.input.fileinputformat.split.minsize", minNumSplits);
		
		JobContext jobContext = null;
		try {
			jobContext = HadoopUtils.instantiateJobContext(configuration, new JobID());
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		
		List<org.apache.hadoop.mapreduce.InputSplit> splits;
		try {
			splits = this.mapreduceInputFormat.getSplits(jobContext);
		} catch (InterruptedException e) {
			throw new IOException("Could not get Splits.", e);
		}
		HadoopInputSplit[] hadoopInputSplits = new HadoopInputSplit[splits.size()];
		
		for(int i = 0; i < hadoopInputSplits.length; i++){
			hadoopInputSplits[i] = new HadoopInputSplit(splits.get(i), jobContext);
		}
		return hadoopInputSplits;
	}
	
	@Override
	public Class<? extends HadoopInputSplit> getInputSplitType() {
		return HadoopInputSplit.class;
	}
	
	@Override
	public void open(HadoopInputSplit split) throws IOException {
		TaskAttemptContext context = null;
		try {
			context = HadoopUtils.instantiateTaskAttemptContext(configuration, new TaskAttemptID());
		} catch(Exception e) {
			throw new RuntimeException(e);
		}
		
		try {
			this.recordReader = this.mapreduceInputFormat
					.createRecordReader(split.getHadoopInputSplit(), context);
			this.recordReader.initialize(split.getHadoopInputSplit(), context);
		} catch (InterruptedException e) {
			throw new IOException("Could not create RecordReader.", e);
		} finally {
			this.fetched = false;
		}
	}
	
	@Override
	public boolean reachedEnd() throws IOException {
		if(!this.fetched) {
			fetchNext();
		}
		return !this.hasNext;
	}
	
	private void fetchNext() throws IOException {
		try {
			this.hasNext = this.recordReader.nextKeyValue();
		} catch (InterruptedException e) {
			throw new IOException("Could not fetch next KeyValue pair.", e);
		} finally {
			this.fetched = true;
		}
	}
	
	@Override
	public Tuple2<K, V> nextRecord(Tuple2<K, V> record) throws IOException {
		if(!this.fetched) {
			fetchNext();
		}
		if(!this.hasNext) {
			return null;
		}
		try {
			record.f0 = this.recordReader.getCurrentKey();
			record.f1 = this.recordReader.getCurrentValue();
		} catch (InterruptedException e) {
			throw new IOException("Could not get KeyValue pair.", e);
		}
		this.fetched = false;
		
		return record;
	}
	
	@Override
	public void close() throws IOException {
		this.recordReader.close();
	}
	
	// --------------------------------------------------------------------------------------------
	//  Helper methods
	// --------------------------------------------------------------------------------------------
	
	private FileBaseStatistics getFileStats(FileBaseStatistics cachedStats, org.apache.hadoop.fs.Path[] hadoopFilePaths,
			ArrayList<FileStatus> files) throws IOException {
		
		long latestModTime = 0L;
		
		// get the file info and check whether the cached statistics are still valid.
		for(org.apache.hadoop.fs.Path hadoopPath : hadoopFilePaths) {
			
			final Path filePath = new Path(hadoopPath.toUri());
			final FileSystem fs = FileSystem.get(filePath.toUri());
			
			final FileStatus file = fs.getFileStatus(filePath);
			latestModTime = Math.max(latestModTime, file.getModificationTime());
			
			// enumerate all files and check their modification time stamp.
			if (file.isDir()) {
				FileStatus[] fss = fs.listStatus(filePath);
				files.ensureCapacity(files.size() + fss.length);
				
				for (FileStatus s : fss) {
					if (!s.isDir()) {
						files.add(s);
						latestModTime = Math.max(s.getModificationTime(), latestModTime);
					}
				}
			} else {
				files.add(file);
			}
		}
		
		// check whether the cached statistics are still valid, if we have any
		if (cachedStats != null && latestModTime <= cachedStats.getLastModificationTime()) {
			return cachedStats;
		}
		
		// calculate the whole length
		long len = 0;
		for (FileStatus s : files) {
			len += s.getLen();
		}
		
		// sanity check
		if (len <= 0) {
			len = BaseStatistics.SIZE_UNKNOWN;
		}
		
		return new FileBaseStatistics(latestModTime, len, BaseStatistics.AVG_RECORD_BYTES_UNKNOWN);
	}
	
	// --------------------------------------------------------------------------------------------
	//  Custom serialization methods
	// --------------------------------------------------------------------------------------------
	
	private void writeObject(ObjectOutputStream out) throws IOException {
		out.writeUTF(this.mapreduceInputFormat.getClass().getName());
		out.writeUTF(this.keyClass.getName());
		out.writeUTF(this.valueClass.getName());
		this.configuration.write(out);
	}
	
	@SuppressWarnings("unchecked")
	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
		String hadoopInputFormatClassName = in.readUTF();
		String keyClassName = in.readUTF();
		String valueClassName = in.readUTF();
		
		org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration();
		configuration.readFields(in);
		
		if(this.configuration == null) {
			this.configuration = configuration;
		}
		
		try {
			this.mapreduceInputFormat = (org.apache.hadoop.mapreduce.InputFormat<K,V>) Class.forName(hadoopInputFormatClassName, true, Thread.currentThread().getContextClassLoader()).newInstance();
		} catch (Exception e) {
			throw new RuntimeException("Unable to instantiate the hadoop input format", e);
		}
		try {
			this.keyClass = (Class<K>) Class.forName(keyClassName, true, Thread.currentThread().getContextClassLoader());
		} catch (Exception e) {
			throw new RuntimeException("Unable to find key class.", e);
		}
		try {
			this.valueClass = (Class<V>) Class.forName(valueClassName, true, Thread.currentThread().getContextClassLoader());
		} catch (Exception e) {
			throw new RuntimeException("Unable to find value class.", e);
		}
	}
	
	// --------------------------------------------------------------------------------------------
	//  ResultTypeQueryable
	// --------------------------------------------------------------------------------------------
	
	@Override
	public TypeInformation<Tuple2<K,V>> getProducedType() {
		return new TupleTypeInfo<Tuple2<K,V>>(new WritableTypeInfo<K>((Class<K>) keyClass), new WritableTypeInfo<V>((Class<V>) valueClass));
	}
}
