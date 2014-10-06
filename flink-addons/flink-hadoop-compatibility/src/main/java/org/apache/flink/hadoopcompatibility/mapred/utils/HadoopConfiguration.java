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

package org.apache.flink.hadoopcompatibility.mapred.utils;

import org.apache.flink.runtime.fs.hdfs.DistributedFileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Map;

/**
 * Merge HadoopConfiguration into JobConf. This is necessary for the HDFS configuration.
 */
public class HadoopConfiguration {

	public static void mergeHadoopConf(JobConf jobConf) {
		org.apache.hadoop.conf.Configuration hadoopConf = DistributedFileSystem.getHadoopConfiguration();
		for (Map.Entry<String, String> e : hadoopConf) {
			jobConf.set(e.getKey(), e.getValue());
		}
	}

	/**
	 * Each task should gets its own jobConf object, when serializing.
	 * @param jobConf the jobConf to write
	 * @param out the outputstream to write to
	 * @throws IOException
	 */
	public static void writeHadoopJobConf(final JobConf jobConf, final ObjectOutputStream out) throws IOException{
		final JobConf clonedConf = WritableUtils.clone(jobConf, new Configuration());
		clonedConf.write(out);
	}
}