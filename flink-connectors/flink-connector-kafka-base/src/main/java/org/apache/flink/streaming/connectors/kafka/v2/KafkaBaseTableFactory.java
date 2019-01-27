/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.flink.streaming.connectors.kafka.v2;

import org.apache.flink.streaming.connectors.kafka.config.StartupMode;
import org.apache.flink.table.util.TableProperties;
import org.apache.flink.util.StringUtils;

import java.util.Iterator;
import java.util.Properties;
import java.util.Set;

/** Kafka TableFactory base. */
public abstract class KafkaBaseTableFactory {

	protected Properties getProperties(
			Set<String> essentialKeys,
			Set<String> optionalKeys,
			TableProperties properties) {
		Properties prop = new Properties();

		Iterator<String> iterator = essentialKeys.iterator();
		while (iterator.hasNext()) {
			String key = iterator.next();
			if (!properties.containsKey(key)) {
				throw new RuntimeException("No sufficient parameters for Kafka. Missing required config: " + key);
			} else {
				prop.put(key, properties.getString(key, null));
			}
		}
		Iterator<String> iterator1 = optionalKeys.iterator();
		while (iterator1.hasNext()) {
			String key = iterator1.next();
			if (properties.containsKey(key)) {
				prop.put(key, properties.getString(key, null));
			}
		}
		String extraConfig = properties.getString(KafkaOptions.EXTRA_CONFIG);
		if (!StringUtils.isNullOrWhitespaceOnly(extraConfig)) {
			String[] configs = extraConfig.split(";");
			for (String config : configs) {
				String[] kv = config.split("=");
				if (null != kv && kv.length == 2) {
					prop.put(kv[0], kv[1]);
				}
			}
		}
		return prop;
	}

	protected StartupMode getStartupMode(TableProperties properties) {
		// TODO: support for batch mode.
		boolean isBatchMode = false;

		StartupMode startupMode = isBatchMode ? StartupMode.EARLIEST : StartupMode.GROUP_OFFSETS;
		String startupModeStr = properties.getString(KafkaOptions.STARTUP_MODE);
		if (!StringUtils.isNullOrWhitespaceOnly(startupModeStr)) {
			if (startupModeStr.equalsIgnoreCase("EARLIEST")) {
				startupMode = StartupMode.EARLIEST;
			} else if (startupModeStr.equalsIgnoreCase("GROUP_OFFSETS")) {
				startupMode = StartupMode.GROUP_OFFSETS;
			} else if (startupModeStr.equalsIgnoreCase("LATEST")) {
				startupMode = StartupMode.LATEST;
			} else if (startupModeStr.equalsIgnoreCase("SPECIFIC_OFFSETS")) {
				startupMode = StartupMode.SPECIFIC_OFFSETS;
			} else if (startupModeStr.equalsIgnoreCase("TIMESTAMP")) {
				startupMode = StartupMode.TIMESTAMP;
			}
		}
		return startupMode;
	}

}
