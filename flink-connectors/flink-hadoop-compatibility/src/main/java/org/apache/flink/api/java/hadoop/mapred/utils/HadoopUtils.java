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

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.GlobalConfiguration;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

/**
 * Utility class to work with Apache Hadoop MapRed classes.
 */
@Internal
public final class HadoopUtils {

	private static final Logger LOG = LoggerFactory.getLogger(HadoopUtils.class);

	/**
	 * Merge HadoopConfiguration into JobConf. This is necessary for the HDFS configuration.
	 */
	public static void mergeHadoopConf(JobConf jobConf) {
		// we have to load the global configuration here, because the HadoopInputFormatBase does not
		// have access to a Flink configuration object
		org.apache.flink.configuration.Configuration flinkConfiguration = GlobalConfiguration.loadConfiguration();

		Configuration hadoopConf = getHadoopConfiguration(flinkConfiguration);
		for (Map.Entry<String, String> e : hadoopConf) {
			if (jobConf.get(e.getKey()) == null) {
				jobConf.set(e.getKey(), e.getValue());
			}
		}
	}

	/**
	 * Returns a new Hadoop Configuration object using the path to the hadoop conf configured
	 * in the main configuration (flink-conf.yaml).
	 * This method is public because its being used in the HadoopDataSource.
	 *
	 * @param flinkConfiguration Flink configuration object
	 * @return A Hadoop configuration instance
	 */
	public static Configuration getHadoopConfiguration(org.apache.flink.configuration.Configuration flinkConfiguration) {

		Configuration retConf = new Configuration();

		// We need to load both core-site.xml and hdfs-site.xml to determine the default fs path and
		// the hdfs configuration
		// Try to load HDFS configuration from Hadoop's own configuration files
		// 1. approach: Flink configuration
		final String hdfsDefaultPath = flinkConfiguration.getString(ConfigConstants
				.HDFS_DEFAULT_CONFIG, null);
		if (hdfsDefaultPath != null) {
			retConf.addResource(new org.apache.hadoop.fs.Path(hdfsDefaultPath));
		} else {
			LOG.debug("Cannot find hdfs-default configuration file");
		}

		final String hdfsSitePath = flinkConfiguration.getString(ConfigConstants.HDFS_SITE_CONFIG, null);
		if (hdfsSitePath != null) {
			retConf.addResource(new org.apache.hadoop.fs.Path(hdfsSitePath));
		} else {
			LOG.debug("Cannot find hdfs-site configuration file");
		}

		// 2. Approach environment variables
		for (String possibleHadoopConfPath : possibleHadoopConfPaths(flinkConfiguration)) {
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
		return retConf;
	}

	/**
	 * Get possible Hadoop conf dir paths, based on environment variables and flink configuration.
	 *
	 * @param flinkConfiguration The flink configuration that may contain the path to Hadoop conf dir.
	 * @return an array of possible paths
	 */
	public static String[] possibleHadoopConfPaths(org.apache.flink.configuration.Configuration flinkConfiguration) {
		String[] possiblePaths = new String[4];
		possiblePaths[0] = flinkConfiguration.getString(ConfigConstants.PATH_HADOOP_CONFIG, null);
		possiblePaths[1] = System.getenv("HADOOP_CONF_DIR");

		if (System.getenv("HADOOP_HOME") != null) {
			possiblePaths[2] = System.getenv("HADOOP_HOME") + "/conf";
			possiblePaths[3] = System.getenv("HADOOP_HOME") + "/etc/hadoop"; // hadoop 2.2
		}
		return Arrays.stream(possiblePaths).filter(Objects::nonNull).toArray(String[]::new);
	}

	/**
	 * Private constructor to prevent instantiation.
	 */
	private HadoopUtils() {
		throw new RuntimeException();
	}
}
