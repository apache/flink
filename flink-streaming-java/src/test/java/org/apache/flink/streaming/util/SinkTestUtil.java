/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.util;

import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.operators.sink.TestSink;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.apache.flink.util.function.FunctionUtils;

import org.hamcrest.Matcher;
import org.hamcrest.collection.IsIterableContainingInAnyOrder;
import org.hamcrest.collection.IsIterableContainingInOrder;
import org.hamcrest.core.IsEqual;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Utilities for testing sink.
 */
public class SinkTestUtil {

	@SuppressWarnings({"unchecked", "rawtypes"})
	public static org.hamcrest.Matcher<Iterable<?>> containStreamElements(Object... items) {
		return new IsIterableContainingInOrder(createStreamElementMatchers(items));
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	public static Matcher<Iterable<?>> containsStreamElementsInAnyOrder(Object... items) {
		return new IsIterableContainingInAnyOrder(createStreamElementMatchers(items));
	}

	@SuppressWarnings("unchecked")
	private static List<Matcher<?>> createStreamElementMatchers(Object... items) {
		final List<Matcher<?>> matchers = new ArrayList<>();
		for (Object item : items) {
			if (item instanceof Watermark) {
				matchers.add(IsEqual.equalTo(item));
			}
			if (item instanceof StreamRecord) {
				StreamRecord<byte[]> streamRecord = (StreamRecord<byte[]>) item;
				matchers.add(StreamRecordMatchers.streamRecord(
						streamRecord.getValue(),
						streamRecord.getTimestamp()));
			}
		}
		return matchers;
	}

	public static List<byte[]> convertStringListToByteArrayList(List<String> input) {
		return input
				.stream()
				.map(FunctionUtils.uncheckedFunction(TestSink.StringCommittableSerializer.INSTANCE::serialize))
				.collect(
						Collectors.toList());
	}
}
