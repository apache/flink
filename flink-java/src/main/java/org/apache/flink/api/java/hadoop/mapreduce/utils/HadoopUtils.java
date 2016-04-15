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

package org.apache.flink.api.java.hadoop.mapreduce.utils;

import java.lang.reflect.Constructor;
import java.util.Map;

import org.apache.flink.annotation.Internal;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;

/**
 * Utility class to work with next generation of Apache Hadoop MapReduce classes.
 */
@Internal
public final class HadoopUtils {
	
	/**
	 * Merge HadoopConfiguration into Configuration. This is necessary for the HDFS configuration.
	 */
	public static void mergeHadoopConf(Configuration configuration) {
		Configuration hadoopConf = org.apache.flink.api.java.hadoop.mapred.utils.HadoopUtils.getHadoopConfiguration();
		
		for (Map.Entry<String, String> e : hadoopConf) {
			if (configuration.get(e.getKey()) == null) {
				configuration.set(e.getKey(), e.getValue());
			}
		}
	}
	
	public static JobContext instantiateJobContext(Configuration configuration, JobID jobId) throws Exception {
		try {
			Class<?> clazz;
			// for Hadoop 1.xx
			if(JobContext.class.isInterface()) {
				clazz = Class.forName("org.apache.hadoop.mapreduce.task.JobContextImpl", true, Thread.currentThread().getContextClassLoader());
			}
			// for Hadoop 2.xx
			else {
				clazz = Class.forName("org.apache.hadoop.mapreduce.JobContext", true, Thread.currentThread().getContextClassLoader());
			}
			Constructor<?> constructor = clazz.getConstructor(Configuration.class, JobID.class);
			JobContext context = (JobContext) constructor.newInstance(configuration, jobId);
			
			return context;
		} catch(Exception e) {
			throw new Exception("Could not create instance of JobContext.");
		}
	}
	
	public static TaskAttemptContext instantiateTaskAttemptContext(Configuration configuration,  TaskAttemptID taskAttemptID) throws Exception {
		try {
			Class<?> clazz;
			// for Hadoop 1.xx
			if(JobContext.class.isInterface()) {
				clazz = Class.forName("org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl");
			}
			// for Hadoop 2.xx
			else {
				clazz = Class.forName("org.apache.hadoop.mapreduce.TaskAttemptContext");
			}
			Constructor<?> constructor = clazz.getConstructor(Configuration.class, TaskAttemptID.class);
			TaskAttemptContext context = (TaskAttemptContext) constructor.newInstance(configuration, taskAttemptID);
			
			return context;
		} catch(Exception e) {
			throw new Exception("Could not create instance of TaskAttemptContext.");
		}
	}

	/**
	 * Private constructor to prevent instantiation.
	 */
	private HadoopUtils() {
		throw new RuntimeException();
	}
}
