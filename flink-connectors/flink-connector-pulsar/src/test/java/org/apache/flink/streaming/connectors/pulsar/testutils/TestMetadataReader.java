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

package org.apache.flink.streaming.connectors.pulsar.testutils;

import org.apache.flink.streaming.connectors.pulsar.internal.PulsarMetadataReader;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions;
import org.apache.flink.streaming.connectors.pulsar.internal.TopicRange;

import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.mockito.stubbing.Answer;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Mock for metadata reader.
 */
public class TestMetadataReader extends PulsarMetadataReader {

	private final List<Set<TopicRange>> mockGetAllTopicsReturnSequence;

	private int getAllTopicsInvCount = 0;

	public TestMetadataReader(
		Map<String, String> caseInsensitiveParams,
		int indexOfThisSubtask,
		int numParallelSubtasks,
		List<Set<TopicRange>> mockGetAllTopicsReturnSequence) throws PulsarClientException {
		super("http://localhost:8080",
			new ClientConfigurationData(),
			"",
			caseInsensitiveParams,
			indexOfThisSubtask,
			numParallelSubtasks);
		this.mockGetAllTopicsReturnSequence = mockGetAllTopicsReturnSequence;
	}

	public Set<TopicRange> getTopicPartitionsAll() {
		return mockGetAllTopicsReturnSequence.get(getAllTopicsInvCount++);
	}

	public static List<Set<TopicRange>> createMockGetAllTopicsSequenceFromFixedReturn(Set<TopicRange> fixed) {
		List<Set<TopicRange>> mockSequence = mock(List.class);
		when(mockSequence.get(anyInt())).thenAnswer((Answer<Set<TopicRange>>) invocation -> fixed);

		return mockSequence;
	}

	public static List<Set<TopicRange>> createMockGetAllTopicsSequenceFromTwoReturns(List<Set<TopicRange>> fixed) {
		List<Set<TopicRange>> mockSequence = mock(List.class);

		when(mockSequence.get(0)).thenAnswer((Answer<Set<TopicRange>>) invocation -> fixed.get(0));
		when(mockSequence.get(1)).thenAnswer((Answer<Set<TopicRange>>) invocation -> fixed.get(1));

		return mockSequence;
	}

	public static int getExpectedSubtaskIndex(TopicRange topicRange, int numTasks) {
		String tp = topicRange.getTopic();
		if (tp.contains(PulsarOptions.PARTITION_SUFFIX)) {
			int pos = tp.lastIndexOf(PulsarOptions.PARTITION_SUFFIX);
			String topicPrefix = tp.substring(0, pos);
			String topicPartitionIndex = tp.substring(
				pos + PulsarOptions.PARTITION_SUFFIX.length());
			if (topicPartitionIndex.matches("0|[1-9]\\d*")) {
				int startIndex = (topicPrefix.hashCode() * 31 & Integer.MAX_VALUE) % numTasks;
				return (startIndex + Integer.valueOf(topicPartitionIndex))
					% numTasks;
			}
		}
		return ((tp.hashCode() * 31) & 0x7FFFFFFF) % numTasks;
	}
}
