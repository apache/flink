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

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * This class helps to do serialization for hadoop Configuration.
 */
@Internal
public class HBaseConfigurationUtil {

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
