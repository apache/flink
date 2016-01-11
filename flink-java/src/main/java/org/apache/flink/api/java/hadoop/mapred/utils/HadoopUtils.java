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


package org.apache.flink.api.java.hadoop.mapred.utils;

import java.io.File;
import java.lang.reflect.Constructor;
import java.util.Map;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.TaskAttemptContext;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class to work with Apache Hadoop MapRed classes.
 */
public final class HadoopUtils {

	private static final Logger LOG = LoggerFactory.getLogger(HadoopUtils.class);

	/**
	 * Merge HadoopConfiguration into JobConf. This is necessary for the HDFS configuration.
	 */
	public static void mergeHadoopConf(JobConf jobConf) {
		org.apache.hadoop.conf.Configuration hadoopConf = getHadoopConfiguration();
		for (Map.Entry<String, String> e : hadoopConf) {
			if (jobConf.get(e.getKey()) == null) {
				jobConf.set(e.getKey(), e.getValue());
			}
		}
	}
	
	public static JobContext instantiateJobContext(JobConf jobConf, JobID jobId) throws Exception {
		try {
			// for Hadoop 1.xx
			Class<?> clazz = null;
			if(!TaskAttemptContext.class.isInterface()) { 
				clazz = Class.forName("org.apache.hadoop.mapred.JobContext", true, Thread.currentThread().getContextClassLoader());
			}
			// for Hadoop 2.xx
			else {
				clazz = Class.forName("org.apache.hadoop.mapred.JobContextImpl", true, Thread.currentThread().getContextClassLoader());
			}
			Constructor<?> constructor = clazz.getDeclaredConstructor(JobConf.class, org.apache.hadoop.mapreduce.JobID.class);
			// for Hadoop 1.xx
			constructor.setAccessible(true);
			JobContext context = (JobContext) constructor.newInstance(jobConf, jobId);
			
			return context;
		} catch(Exception e) {
			throw new Exception("Could not create instance of JobContext.", e);
		}
	}
	
	public static TaskAttemptContext instantiateTaskAttemptContext(JobConf jobConf,  TaskAttemptID taskAttemptID) throws Exception {
		try {
			// for Hadoop 1.xx
			Class<?> clazz = null;
			if(!TaskAttemptContext.class.isInterface()) { 
				clazz = Class.forName("org.apache.hadoop.mapred.TaskAttemptContext", true, Thread.currentThread().getContextClassLoader());
			}
			// for Hadoop 2.xx
			else {
				clazz = Class.forName("org.apache.hadoop.mapred.TaskAttemptContextImpl", true, Thread.currentThread().getContextClassLoader());
			}
			Constructor<?> constructor = clazz.getDeclaredConstructor(JobConf.class, TaskAttemptID.class);
			// for Hadoop 1.xx
			constructor.setAccessible(true);
			TaskAttemptContext context = (TaskAttemptContext) constructor.newInstance(jobConf, taskAttemptID);
			return context;
		} catch(Exception e) {
			throw new Exception("Could not create instance of TaskAttemptContext.", e);
		}
	}

	/**
	 * Returns a new Hadoop Configuration object using the path to the hadoop conf configured
	 * in the main configuration (flink-conf.yaml).
	 * This method is public because its being used in the HadoopDataSource.
	 */
	public static org.apache.hadoop.conf.Configuration getHadoopConfiguration() {
		Configuration retConf = new org.apache.hadoop.conf.Configuration();

		// We need to load both core-site.xml and hdfs-site.xml to determine the default fs path and
		// the hdfs configuration
		// Try to load HDFS configuration from Hadoop's own configuration files
		// 1. approach: Flink configuration
		final String hdfsDefaultPath = GlobalConfiguration.getString(ConfigConstants
				.HDFS_DEFAULT_CONFIG, null);
		if (hdfsDefaultPath != null) {
			retConf.addResource(new org.apache.hadoop.fs.Path(hdfsDefaultPath));
		} else {
			LOG.debug("Cannot find hdfs-default configuration file");
		}

		final String hdfsSitePath = GlobalConfiguration.getString(ConfigConstants.HDFS_SITE_CONFIG, null);
		if (hdfsSitePath != null) {
			retConf.addResource(new org.apache.hadoop.fs.Path(hdfsSitePath));
		} else {
			LOG.debug("Cannot find hdfs-site configuration file");
		}

		// 2. Approach environment variables
		String[] possibleHadoopConfPaths = new String[4];
		possibleHadoopConfPaths[0] = GlobalConfiguration.getString(ConfigConstants.PATH_HADOOP_CONFIG, null);
		possibleHadoopConfPaths[1] = System.getenv("HADOOP_CONF_DIR");

		if (System.getenv("HADOOP_HOME") != null) {
			possibleHadoopConfPaths[2] = System.getenv("HADOOP_HOME")+"/conf";
			possibleHadoopConfPaths[3] = System.getenv("HADOOP_HOME")+"/etc/hadoop"; // hadoop 2.2
		}

		for (String possibleHadoopConfPath : possibleHadoopConfPaths) {
			if (possibleHadoopConfPath != null) {
				if (new File(possibleHadoopConfPath).exists()) {
					if (new File(possibleHadoopConfPath + "/core-site.xml").exists()) {
						retConf.addResource(new org.apache.hadoop.fs.Path(possibleHadoopConfPath + "/core-site.xml"));

						if (LOG.isDebugEnabled()) {
							LOG.debug("Adding " + possibleHadoopConfPath + "/core-site.xml to hadoop configuration");
						}
					}
					if (new File(possibleHadoopConfPath + "/hdfs-site.xml").exists()) {
						retConf.addResource(new org.apache.hadoop.fs.Path(possibleHadoopConfPath + "/hdfs-site.xml"));

						if (LOG.isDebugEnabled()) {
							LOG.debug("Adding " + possibleHadoopConfPath + "/hdfs-site.xml to hadoop configuration");
						}
					}
				}
			}
		}
		return retConf;
	}

	/**
	 * Private constructor to prevent instantiation.
	 */
	private HadoopUtils() {
		throw new RuntimeException();
	}
}
