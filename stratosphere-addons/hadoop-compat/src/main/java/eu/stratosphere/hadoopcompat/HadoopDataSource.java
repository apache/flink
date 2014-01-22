package eu.stratosphere.hadoopcompat;

import java.lang.annotation.Annotation;

import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;

import eu.stratosphere.api.common.operators.GenericDataSource;
import eu.stratosphere.api.common.operators.Operator;
import eu.stratosphere.util.Visitor;

public class HadoopDataSource extends GenericDataSource<HadoopInputFormatWrapper<?,?,?>> {

	private static String DEFAULT_NAME = "<Unnamed File Data Source>";
	
	protected final String filePath;
	private String name;
	
	
	public HadoopDataSource(
			HadoopInputFormatWrapper<?, ?, ?> format, String filePath, String name) {
		super(format,name);
		this.filePath=filePath;
		format.setFilePath(filePath);
		this.name = name;
	}
	
	public HadoopDataSource(
			InputFormat hadoopFormat, JobConf jobConf, String filePath, String name) {
		super(new HadoopInputFormatWrapper(hadoopFormat, jobConf, filePath),name);
		this.filePath=filePath;
		this.name = name;
	}
	
	public HadoopDataSource(HadoopInputFormatWrapper<?, ?, ?> format, String filePath) {
		this(format,filePath,DEFAULT_NAME);
	}

	public String getFilePath() {
		return this.filePath;
	}
	
	// --------------------------------------------------------------------------------------------
	
	public String toString() {
		return this.filePath;
	}
	

}
