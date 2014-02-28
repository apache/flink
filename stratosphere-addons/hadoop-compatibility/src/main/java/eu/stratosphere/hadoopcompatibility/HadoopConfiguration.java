package eu.stratosphere.hadoopcompatibility;

import eu.stratosphere.runtime.fs.hdfs.DistributedFileSystem;
import org.apache.hadoop.mapred.JobConf;

import java.util.Map;

/**
 * merge hadoopConf into jobConf. This is necessary for the hdfs configuration

 */

public class HadoopConfiguration {
	public static void mergeHadoopConf(JobConf jobConf) {
		org.apache.hadoop.conf.Configuration hadoopConf = DistributedFileSystem.getHadoopConfiguration();
		for (Map.Entry<String, String> e : hadoopConf) {
			jobConf.set(e.getKey(), e.getValue());
		}
	}
}
