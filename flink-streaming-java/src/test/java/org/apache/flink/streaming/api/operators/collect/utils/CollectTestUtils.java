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

package org.apache.flink.streaming.api.operators.collect.utils;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Utilities for testing collecting mechanism.
 */
public class CollectTestUtils {

	public static <T> List<byte[]> toBytesList(List<T> values, TypeSerializer<T> serializer) {
		List<byte[]> ret = new ArrayList<>();
		for (T value : values) {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			DataOutputViewStreamWrapper wrapper = new DataOutputViewStreamWrapper(baos);
			try {
				serializer.serialize(value, wrapper);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
			ret.add(baos.toByteArray());
		}
		return ret;
	}
}
