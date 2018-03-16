/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.apache.flink.python.api.functions.util;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.python.api.streaming.util.SerializationUtils;
import org.apache.flink.python.api.streaming.util.SerializationUtils.Serializer;

/**
 * Utility function to serialize values, usually directly from data sources.
 */
public class SerializerMap<IN> implements MapFunction<IN, byte[]> {
	private transient Serializer<IN> serializer;

	@Override
	@SuppressWarnings("unchecked")
	public byte[] map(IN value) throws Exception {
		if (serializer == null) {
			serializer = SerializationUtils.getSerializer(value);
		}
		return serializer.serialize(value);
	}
}
