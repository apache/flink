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

package org.apache.flink.streaming.connectors.pulsar.internal;

import org.apache.flink.annotation.Internal;

import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.Range;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Utilities for source sink options parsing.
 */
@Internal
public class SourceSinkUtils {

	public static Map<String, String> validateStreamSourceOptions(Map<String, String> parameters) {
		Map<String, String> caseInsensitiveParams = parameters.entrySet().stream()
			.collect(Collectors.toMap(t -> t.getKey().toLowerCase(Locale.ROOT), t -> t.getValue()));

		return validateSourceOptions(caseInsensitiveParams);
	}

	private static Map<String, String> validateSourceOptions(Map<String, String> caseInsensitiveParams) {
		Map<String, String> topicOptions = caseInsensitiveParams.entrySet().stream()
			.filter(t -> PulsarOptions.TOPIC_OPTION_KEYS.contains(t.getKey()))
			.collect(Collectors.toMap(map -> map.getKey(), map -> map.getValue()));

		if (topicOptions.isEmpty() || topicOptions.size() > 1) {
			throw new IllegalArgumentException(
				"You should specify topic(s) using one of the topic options: " +
					StringUtils.join(PulsarOptions.TOPIC_OPTION_KEYS, ","));
		}

		for (Map.Entry<String, String> topicEntry : topicOptions.entrySet()) {
			String key = topicEntry.getKey();
			String value = topicEntry.getValue();
			if (key.equals("topic")) {
				if (value.contains(",")) {
					throw new IllegalArgumentException(
						"Use `topics` instead of `topic` for multi topic read");
				}
			} else if (key.equals("topics")) {
				List<String> topics = Arrays.asList(value.split(",")).stream()
					.map(String::trim).filter(t -> !t.isEmpty()).collect(Collectors.toList());
				if (topics.isEmpty()) {
					throw new IllegalArgumentException(
						"No topics is specified for read with option" + value);
				}
			} else {
				if (value.trim().length() == 0) {
					throw new IllegalArgumentException("TopicsPattern is empty");
				}
			}
		}
		return caseInsensitiveParams;
	}

	public static boolean belongsTo(TopicRange topicRange, int numParallelSubtasks, int index) {
		String topic = topicRange.getTopic();
		if (topic.contains(PulsarOptions.PARTITION_SUFFIX)) {
			int pos = topic.lastIndexOf(PulsarOptions.PARTITION_SUFFIX);
			String topicPrefix = topic.substring(0, pos);
			String topicPartitionIndex = topic.substring(
				pos + PulsarOptions.PARTITION_SUFFIX.length());
			if (topicPartitionIndex.matches("0|[1-9]\\d*")) {
				int startIndex =
					(topicPrefix.hashCode() * 31 & Integer.MAX_VALUE) % numParallelSubtasks;
				return (startIndex + Integer.valueOf(topicPartitionIndex))
					% numParallelSubtasks == index;
			}
		}
		return (topic.hashCode() * 31 & Integer.MAX_VALUE) % numParallelSubtasks == index;
	}

	public static long getPartitionDiscoveryIntervalInMillis(Map<String, String> parameters) {
		String interval = parameters.getOrDefault(
			PulsarOptions.PARTITION_DISCOVERY_INTERVAL_MS_OPTION_KEY,
			"-1");
		return Long.parseLong(interval);
	}

	public static int getSendTimeoutMs(Map<String, String> parameters) {
		String interval = parameters.getOrDefault(PulsarOptions.SEND_TIMEOUT_MS, "30000");
		return Integer.parseInt(interval);
	}

	public static int getPollTimeoutMs(Map<String, String> parameters) {
		String interval = parameters.getOrDefault(
			PulsarOptions.POLL_TIMEOUT_MS_OPTION_KEY,
			"120000");
		return Integer.parseInt(interval);
	}

	public static boolean getUseMetrics(Map<String, String> parameters) {
		String useMetrics = parameters.getOrDefault(PulsarOptions.KEY_DISABLED_METRICS, "false");
		return Boolean.parseBoolean(useMetrics);
	}

	public static int getCommitMaxRetries(Map<String, String> parameters) {
		String retries = parameters.getOrDefault(PulsarOptions.COMMIT_MAX_RETRIES, "3");
		return Integer.parseInt(retries);
	}

	public static int getClientCacheSize(Map<String, String> parameters) {
		String size = parameters.getOrDefault(PulsarOptions.CLIENT_CACHE_SIZE_OPTION_KEY, "5");
		return Integer.parseInt(size);
	}

	public static boolean flushOnCheckpoint(Map<String, String> parameters) {
		String b = parameters.getOrDefault(PulsarOptions.FLUSH_ON_CHECKPOINT_OPTION_KEY, "true");
		return Boolean.parseBoolean(b);
	}

	public static boolean failOnWrite(Map<String, String> parameters) {
		String b = parameters.getOrDefault(PulsarOptions.FAIL_ON_WRITE_OPTION_KEY, "false");
		return Boolean.parseBoolean(b);
	}

	public static long getTransactionTimeout(Map<String, String> parameters) {
		String value = parameters.getOrDefault(PulsarOptions.TRANSACTION_TIMEOUT, "3600000");
		return Long.parseLong(value);
	}

	public static long getMaxBlockTimeMs(Map<String, String> parameters) {
		String value = parameters.getOrDefault(PulsarOptions.MAX_BLOCK_TIME_MS, "100000");
		return Long.parseLong(value);
	}

	public static Map<String, Object> getReaderParams(Map<String, String> parameters) {
		return parameters.keySet().stream()
			.filter(k -> k.startsWith(PulsarOptions.PULSAR_READER_OPTION_KEY_PREFIX))
			.collect(Collectors.toMap(k -> k.substring(PulsarOptions.PULSAR_READER_OPTION_KEY_PREFIX
				.length()), k -> parameters.get(k)));
	}

	public static Map<String, String> toCaceInsensitiveParams(Map<String, String> parameters) {
		return parameters.entrySet().stream()
			.collect(Collectors.toMap(t -> t.getKey().toLowerCase(Locale.ROOT), t -> t.getValue()));
	}

	public static Map<String, Object> getProducerParams(Map<String, String> parameters) {
		return parameters.keySet().stream()
			.filter(k -> k.startsWith(PulsarOptions.PULSAR_PRODUCER_OPTION_KEY_PREFIX))
			.collect(Collectors.toMap(k -> k.substring(PulsarOptions.PULSAR_PRODUCER_OPTION_KEY_PREFIX
				.length()), k -> parameters.get(k)));
	}

	/**
	 * Get shard information of the task
	 * Fragmentation rules,
	 * Can be divided equally: each subtask handles the same range of tasks
	 * Not evenly divided: each subtask first processes the tasks in the same range,
	 * and the remainder part is added to the tasks starting at index 0 until it is used up.
	 *
	 * @param countOfSubTasks total subtasks
	 * @param indexOfSubTasks current subtask index on subtasks
	 *
	 * @return task range
	 */
	public static Range distributeRange(int countOfSubTasks, int indexOfSubTasks) {
		int countOfKey = SerializableRange.fullRangeEnd + 1;
		int part = countOfKey / countOfSubTasks;
		int remainder = countOfKey % countOfSubTasks;

		int subTasksStartKey, subTasksEndKey;
		if (indexOfSubTasks < remainder) {
			part++;
			subTasksStartKey = indexOfSubTasks * part;
			subTasksEndKey = indexOfSubTasks * part + part;
		} else {
			subTasksStartKey = indexOfSubTasks * part + remainder;
			subTasksEndKey = indexOfSubTasks * part + part + remainder;
		}

		subTasksEndKey--;

		return Range.of(subTasksStartKey, subTasksEndKey);
	}
}
