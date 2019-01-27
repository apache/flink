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

package org.apache.flink.connectors.hbase.util;

import org.apache.flink.annotation.Internal;

import java.io.IOException;

/**
 * This class helps to do serialization for hadoop Configuration.
 */
@Internal
public class HBaseConfigurationUtil {

	public static byte[] serializeConfiguration(org.apache.hadoop.conf.Configuration conf) throws IOException {
		return WritableSerializer.serializeWritable(conf);
	}

	public static org.apache.hadoop.conf.Configuration deserializeConfiguration(
			byte[] serializedConfig,
			org.apache.hadoop.conf.Configuration targetConfig) throws IOException {
		if (null == targetConfig) {
			targetConfig = new org.apache.hadoop.conf.Configuration();
		}
		WritableSerializer.deserializeWritable(targetConfig, serializedConfig);
		return targetConfig;
	}
}
