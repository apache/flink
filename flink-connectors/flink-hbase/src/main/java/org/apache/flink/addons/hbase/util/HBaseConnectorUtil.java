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

package org.apache.flink.addons.hbase.util;

import org.apache.flink.addons.hbase.AbstractTableInputFormat;
import org.apache.flink.addons.hbase.TableInputSplit;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Util class for HBase connector module.
 */
public class HBaseConnectorUtil {

	protected static final Logger LOG = LoggerFactory.getLogger(AbstractTableInputFormat.class);

	/**
	 * Create an {@link HTable} instance and set it into this format.
	 */
	public static HTable createTable(String tableName, org.apache.hadoop.conf.Configuration hConf) {
		LOG.info("Initializing HBaseConfiguration");
		if (hConf == null) {
			//use files found in the classpath
			hConf = HBaseConfiguration.create();
		}

		try {
			return new HTable(hConf, tableName);
		} catch (Exception e) {
			LOG.error("Error instantiating a new HTable instance", e);
		}
		return null;
	}

	/**
	 * Log {@link TableInputSplit}.
	 *
	 * @param action action taken, for example open or created.
	 * @param split table input split.
	 * @param caller where the method is called, this can be class name
	 */
	public static void logSplitInfo(String action, TableInputSplit split, String caller) {
		int splitId = split.getSplitNumber();
		String splitStart = Bytes.toString(split.getStartRow());
		String splitEnd = Bytes.toString(split.getEndRow());
		String splitStartKey = splitStart.isEmpty() ? "-" : splitStart;
		String splitStopKey = splitEnd.isEmpty() ? "-" : splitEnd;
		String[] hostnames = split.getHostnames();
		LOG.info("{} split (this={})[{}|{}|{}|{}]", action, caller, splitId, hostnames, splitStartKey, splitStopKey);
	}
}
