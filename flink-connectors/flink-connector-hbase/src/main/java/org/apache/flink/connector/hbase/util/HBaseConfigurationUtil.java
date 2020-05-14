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

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;

/**
 * This class helps to do serialization for hadoop Configuration and HBase-related classes.
 */
@Internal
public class HBaseConfigurationUtil {

	private static final Logger LOG = LoggerFactory.getLogger(HBaseConfigurationUtil.class);

	public static Configuration getHBaseConfiguration() {

		// Instantiate an HBaseConfiguration to load the hbase-default.xml and hbase-site.xml from the classpath.
		Configuration result = HBaseConfiguration.create();
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

	/**
	 * Serialize a Hadoop {@link Configuration} into byte[].
	 */
	public static byte[] serializeConfiguration(Configuration conf) {
		try {
			return serializeWritable(conf);
		} catch (IOException e) {
			throw new RuntimeException("Encounter an IOException when serialize the Configuration.", e);
		}
	}

	/**
	 * Deserialize a Hadoop {@link Configuration} from byte[].
	 * Deserialize configs to {@code targetConfig} if it is set.
	 */
	public static Configuration deserializeConfiguration(byte[] serializedConfig, Configuration targetConfig) {
		if (null == targetConfig) {
			targetConfig = new Configuration();
		}
		try {
			deserializeWritable(targetConfig, serializedConfig);
		} catch (IOException e) {
			throw new RuntimeException("Encounter an IOException when deserialize the Configuration.", e);
		}
		return targetConfig;
	}

	/**
	 * Serialize writable byte[].
	 *
	 * @param <T>      the type parameter
	 * @param writable the writable
	 * @return the byte [ ]
	 * @throws IOException the io exception
	 */
	private static <T extends Writable> byte[] serializeWritable(T writable) throws IOException {
		Preconditions.checkArgument(writable != null);

		ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
		DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream);
		writable.write(outputStream);
		return byteArrayOutputStream.toByteArray();
	}

	/**
	 * Deserialize writable.
	 *
	 * @param <T>      the type parameter
	 * @param writable the writable
	 * @param bytes    the bytes
	 * @throws IOException the io exception
	 */
	private static <T extends Writable> void deserializeWritable(T writable, byte[] bytes)
		throws IOException {
		Preconditions.checkArgument(writable != null);
		Preconditions.checkArgument(bytes != null);

		ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
		DataInputStream dataInputStream = new DataInputStream(byteArrayInputStream);
		writable.readFields(dataInputStream);
	}
}
