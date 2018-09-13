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

package org.apache.flink.runtime.io.network.util;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer;

import org.junit.Assert;

import java.util.ArrayDeque;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasProperty;

/**
 * Utility class to help deserialization for testing.
 */
public final class DeserializationUtils {

	/**
	 * Iterates over the provided records to deserialize, verifies the results and stats
	 * the number of full records.
	 *
	 * @param records records to be deserialized
	 * @param deserializer the record deserializer
	 * @param mustBeFullRecords if set, fails if the deserialized records contain partial records
	 * @return the number of full deserialized records
	 */
	public static <T extends IOReadableWritable> int deserializeRecords(
			ArrayDeque<T> records,
			RecordDeserializer<T> deserializer,
			boolean mustBeFullRecords) throws Exception {
		int deserializedRecords = 0;

		while (!records.isEmpty()) {
			T expected = records.poll();
			@SuppressWarnings("unchecked")
			T actual = (T) expected.getClass().newInstance();

			RecordDeserializer.DeserializationResult deserializationResult =
				deserializer.getNextRecord(actual);
			if (mustBeFullRecords) {
				assertThat(deserializationResult, hasProperty("fullRecord", equalTo(true)));
			}
			if (deserializationResult.isFullRecord()) {
				Assert.assertEquals(expected, actual);
				deserializedRecords++;
			} else {
				records.addFirst(expected);
				break;
			}
		}

		return deserializedRecords;
	}
}
