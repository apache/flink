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

import com.google.common.collect.ImmutableList;

import java.util.List;

public class PulsarOptions {

	public static final String TOPIC_ATTRIBUTE_NAME = "__topic";
	public static final String KEY_ATTRIBUTE_NAME = "__key";
	public static final String MESSAGE_ID_NAME = "__messageId";
	public static final String PUBLISH_TIME_NAME = "__publishTime";
	public static final String EVENT_TIME_NAME = "__eventTime";

	public static final List<String> META_FIELD_NAMES = ImmutableList.of(
		TOPIC_ATTRIBUTE_NAME,
		KEY_ATTRIBUTE_NAME,
		MESSAGE_ID_NAME,
		PUBLISH_TIME_NAME,
		EVENT_TIME_NAME);

	public static final String DEFAULT_PARTITIONS = "tableDefaultPartitions";
}
