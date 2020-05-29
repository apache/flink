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

package org.apache.flink.connector.hbase.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * Utility class for working with HBase-related classes. This should only be used if HBase
 * is on the classpath.
 */
public class HBaseUtils {

	private static final Logger LOG = LoggerFactory.getLogger(HBaseUtils.class);

	@SuppressWarnings("deprecation")
	public static Configuration getHBaseConfiguration() {

		// Instantiate an HBaseConfiguration to load the hbase-default.xml and hbase-site.xml from the classpath.
		Configuration result = new HBaseConfiguration().create();
		boolean foundHBaseConfiguration = false;

		// We need to load both hbase-default.xml and hbase-site.xml to the hbase configuration
		// The properties of a newly added resource will override the ones in previous resources, so a configuration
		// file with higher priority should be added later.

		// Approach 1: HBASE_HOME environment variables
		String possibleHBaseConfPath = null;

		final String hbaseHome = System.getenv("HBASE_HOME");
		if (hbaseHome != null) {
			LOG.debug("Searching HBase configuration files in HBASE_HOME: {}", hbaseHome);
			possibleHBaseConfPath = hbaseHome + "/conf";
		}

		if (possibleHBaseConfPath != null) {
			foundHBaseConfiguration = addHBaseConfIfFound(result, possibleHBaseConfPath);
		}

		// Approach 2: HBASE_CONF_DIR environment variable
		String hbaseConfDir = System.getenv("HBASE_CONF_DIR");
		if (hbaseConfDir != null) {
			LOG.debug("Searching HBase configuration files in HBASE_CONF_DIR: {}", hbaseConfDir);
			foundHBaseConfiguration = addHBaseConfIfFound(result, hbaseConfDir) || foundHBaseConfiguration;
		}

		if (!foundHBaseConfiguration) {
			LOG.warn("Could not find HBase configuration via any of the supported methods " +
				"(Flink configuration, environment variables).");
		}

		return result;
	}

	/**
	 * Search HBase configuration files in the given path, and add them to the configuration if found.
	 */
	private static boolean addHBaseConfIfFound(Configuration configuration, String possibleHBaseConfPath) {
		boolean foundHBaseConfiguration = false;
		if (new File(possibleHBaseConfPath).exists()) {
			if (new File(possibleHBaseConfPath + "/hbase-default.xml").exists()) {
				configuration.addResource(new org.apache.hadoop.fs.Path(possibleHBaseConfPath + "/hbase-default.xml"));
				LOG.debug("Adding " + possibleHBaseConfPath + "/hbase-default.xml to hbase configuration");
				foundHBaseConfiguration = true;
			}
			if (new File(possibleHBaseConfPath + "/hbase-site.xml").exists()) {
				configuration.addResource(new org.apache.hadoop.fs.Path(possibleHBaseConfPath + "/hbase-site.xml"));
				LOG.debug("Adding " + possibleHBaseConfPath + "/hbase-site.xml to hbase configuration");
				foundHBaseConfiguration = true;
			}
		}
		return foundHBaseConfiguration;
	}
}
