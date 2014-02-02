package eu.stratosphere.hadoopcompatibility;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Map.Entry;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.util.ReflectionUtils;

import eu.stratosphere.api.common.io.InputFormat;
import eu.stratosphere.api.common.io.statistics.BaseStatistics;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.hadoopcompatibility.datatypes.HadoopTypeConverter;
import eu.stratosphere.runtime.fs.hdfs.DistributedFileSystem;
import eu.stratosphere.types.Record;

public class HadoopInputFormatWrapper<K, V> implements InputFormat<Record, HadoopInputSplitWrapper> {

	private static final long serialVersionUID = 1L;

	public org.apache.hadoop.mapred.InputFormat<K, V> hadoopInputFormat;
	public HadoopTypeConverter<K,V> converter;
	private String hadoopInputFormatName;
	public JobConf jobConf;
	public transient K key;
	public transient V value;
	public RecordReader<K, V> recordReader;
	private boolean fetched = false;
	private boolean hasNext;
		
	public HadoopInputFormatWrapper() {
		super();
	}
	
	public HadoopInputFormatWrapper(org.apache.hadoop.mapred.InputFormat<K,V> hadoopInputFormat, JobConf job, HadoopTypeConverter<K,V> conv) {
		super();
		this.hadoopInputFormat = hadoopInputFormat;
		this.hadoopInputFormatName = hadoopInputFormat.getClass().getName();
		this.converter = conv;
		// merge hadoopConf into jobConf. This is necessary for the hdfs configuration
		org.apache.hadoop.conf.Configuration hadoopConf = DistributedFileSystem.getHadoopConfiguration();
		for (Entry<String, String> e : hadoopConf) {
			job.set(e.getKey(), e.getValue());
	    }
		this.jobConf = job;
	}

	@Override
	public void configure(Configuration parameters) {
		
	}

	@Override
	public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
		return null;
	}

	@Override
	public HadoopInputSplitWrapper[] createInputSplits(int minNumSplits)
			throws IOException {
		org.apache.hadoop.mapred.InputSplit[] splitArray = hadoopInputFormat.getSplits(jobConf, minNumSplits);
		HadoopInputSplitWrapper[] hiSplit = new HadoopInputSplitWrapper[splitArray.length];
		for(int i=0;i<splitArray.length;i++){
			hiSplit[i] = new HadoopInputSplitWrapper(splitArray[i], jobConf);
		}
		return hiSplit;
	}

	@Override
	public Class<? extends HadoopInputSplitWrapper> getInputSplitType() {
		return HadoopInputSplitWrapper.class;
	}

	@Override
	public void open(HadoopInputSplitWrapper split) throws IOException {
		this.recordReader = this.hadoopInputFormat.getRecordReader(split.getHadoopInputSplit(), jobConf, new DummyHadoopReporter());
		key = this.recordReader.createKey();
		value = this.recordReader.createValue();
		this.fetched = false;
	}

	private void fetchNext() throws IOException {
		hasNext = this.recordReader.next(key, value);
		fetched = true;
	}
	
	@Override
	public boolean reachedEnd() throws IOException {
		if(!fetched) {
			fetchNext();
		}
		return !hasNext;
	}

	@Override
	public boolean nextRecord(Record record) throws IOException {
		if(!fetched) {
			fetchNext();
		}
		if(!hasNext) {
			return false;
		}
		converter.convert(record, key, value);
		fetched = false;
		return true;
	}

	@Override
	public void close() throws IOException {
		this.recordReader.close();
	}
	
	/**
	 * Custom serialization methods.
	 *  @see http://docs.oracle.com/javase/7/docs/api/java/io/Serializable.html
	 */
	private void writeObject(ObjectOutputStream out) throws IOException {
		out.writeUTF(hadoopInputFormatName);
		jobConf.write(out);
		out.writeObject(converter);
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    	hadoopInputFormatName = in.readUTF();
    	if(jobConf == null) {
    		jobConf = new JobConf();
    	}
    	jobConf.readFields(in);
        try {
			this.hadoopInputFormat = (org.apache.hadoop.mapred.InputFormat<K,V>) Class.forName(this.hadoopInputFormatName).newInstance();
		} catch (Exception e) {
			throw new RuntimeException("Unable to instantiate the hadoop input format", e);
		}
        ReflectionUtils.setConf(hadoopInputFormat, jobConf);
        converter = (HadoopTypeConverter<K,V>) in.readObject();
    }
	
	public void setJobConf(JobConf job) {
		this.jobConf = job;
	}
		

	public org.apache.hadoop.mapred.InputFormat<K,V> getHadoopInputFormat() {
		return hadoopInputFormat;
	}
	
	public void setHadoopInputFormat(org.apache.hadoop.mapred.InputFormat<K,V> hadoopInputFormat) {
		this.hadoopInputFormat = hadoopInputFormat;
	}
	
	public JobConf getJobConf() {
		return jobConf;
	}
}
