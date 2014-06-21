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

package eu.stratosphere.hadoopcompatibility.mapred.utils;

import java.lang.reflect.Constructor;
import java.util.Map;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.TaskAttemptContext;
import org.apache.hadoop.mapred.TaskAttemptID;

import eu.stratosphere.runtime.fs.hdfs.DistributedFileSystem;


public class HadoopUtils {
	
	/**
	 * Merge HadoopConfiguration into JobConf. This is necessary for the HDFS configuration.
	 */
	public static void mergeHadoopConf(JobConf jobConf) {
		org.apache.hadoop.conf.Configuration hadoopConf = DistributedFileSystem.getHadoopConfiguration();
		for (Map.Entry<String, String> e : hadoopConf) {
			jobConf.set(e.getKey(), e.getValue());
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
}
