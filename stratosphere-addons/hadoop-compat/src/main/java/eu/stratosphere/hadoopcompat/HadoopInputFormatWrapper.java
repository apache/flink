package eu.stratosphere.hadoopcompat;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;

import eu.stratosphere.api.common.io.InputFormat;
import eu.stratosphere.api.common.io.statistics.BaseStatistics;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.hadoopcompat.datatypes.HadoopTypeConverter;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.types.Value;

public class HadoopInputFormatWrapper<OT,K,V> implements InputFormat<OT, HadoopInputSplitWrapper> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public transient org.apache.hadoop.mapred.InputFormat<K, V> hadoopInputFormat;
	public transient HadoopTypeConverter converter;
	private String hadoopInputFormatName;
	public transient JobConf jobConf;
	public transient K key;
	public transient V value;
	private String filePath;
	public RecordReader recordReader;
	private boolean end=false;
		
	public HadoopInputFormatWrapper() {
		super();
	}
	
	public HadoopInputFormatWrapper(org.apache.hadoop.mapred.InputFormat<K,V> hadoopInputFormat, JobConf job) {
		super();
		this.hadoopInputFormat = hadoopInputFormat;
		this.hadoopInputFormatName = hadoopInputFormat.getClass().getName();
		this.jobConf = job;
		ReflectionUtils.setConf(this.hadoopInputFormat, jobConf);
		// TODO Auto-generated constructor stub
	}

	@Override
	public void configure(Configuration parameters) {
		
	}

	@Override
	public BaseStatistics getStatistics(BaseStatistics cachedStatistics)
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public HadoopInputSplitWrapper[] createInputSplits(int minNumSplits)
			throws IOException {
		org.apache.hadoop.mapred.InputSplit[] splitArray = hadoopInputFormat.getSplits(jobConf, minNumSplits);
		HadoopInputSplitWrapper[] hiSplit = new HadoopInputSplitWrapper[splitArray.length];
		for(int i=0;i<splitArray.length;i++)
		{
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
		this.recordReader = this.hadoopInputFormat.getRecordReader(split.getHadoopInputSplit(), jobConf, new HadoopReporter());
		key= (K)this.recordReader.createKey();
		value=(V)this.recordReader.createValue();
		this.end=false;
	}

	@Override
	public boolean reachedEnd() throws IOException {
		return this.end;
	}

	@Override
	public boolean nextRecord(OT record) throws IOException {
//		Object key,value;
		if(converter==null)
		{
			converter = new HadoopTypeConverter();
		}
		
		if(this.recordReader.next(key, value))
		{
			Value value1 = converter.convert(key);
			Value value2 = converter.convert(value);
			((Record)record).setField(0, value1); 
			((Record)record).setField(1, value2); 
			return true;
		}
		this.end=true;
		return false;
	}

	@Override
	public void close() throws IOException {
		
	}
	
	
	//****Serialization Methods****
	
	
	private void writeObject(ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
//        this.job.write(out);
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        this.jobConf = new JobConf();
        
        try {
			this.hadoopInputFormat = (org.apache.hadoop.mapred.InputFormat<K,V>) Class.forName(this.hadoopInputFormatName).newInstance();
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}
        this.jobConf.setInputFormat(this.hadoopInputFormat.getClass());
        Path path = new Path(this.filePath);
        path = path.getFileSystem(jobConf).makeQualified(path);
        String dirStr = StringUtils.escapeString(path.toString());
        String dirs = jobConf.get("mapred.input.dir");
        jobConf.set("mapred.input.dir", dirs == null ? dirStr : dirs + "," + dirStr);
        ReflectionUtils.setConf(this.hadoopInputFormat, jobConf);
        
//        this.hadoopInputFormat = (org.apache.hadoop.mapred.InputFormat<K,V>)in.readObject();
//        this.job.readFields(in);
    }
	
	
	
	//****getters and setters****
	public void setFilePath(String path)
	{
		this.filePath=path;
	}
	
	public void setJobConf(JobConf job)
	{
		this.jobConf = job;
	}
		

	public org.apache.hadoop.mapred.InputFormat<K, V> getHadoopInputFormat() {
		return hadoopInputFormat;
	}
	
	public void setHadoopInputFormat(
			org.apache.hadoop.mapred.InputFormat<K, V> hadoopInputFormat) {
		this.hadoopInputFormat = hadoopInputFormat;
	}
	
	public JobConf getJobConf() {
		return jobConf;
	}
	
	public String getFilePath() {
		return filePath;
	}
}
