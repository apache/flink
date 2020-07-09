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

package org.apache.flink.streaming.connectors.kafka.table;

import org.apache.flink.annotation.Internal;

/**
 * Kafka sink semantic Enum.
 * */
@Internal
public enum KafkaSinkSemantic {
	/**
	 * Semantic.EXACTLY_ONCE the Flink producer will write all messages in a Kafka transaction that will be
	 * committed to Kafka on a checkpoint.
	 */
	EXACTLY_ONCE,
	/**
	 * Semantic.AT_LEAST_ONCE the Flink producer will wait for all outstanding messages in the Kafka buffers
	 * to be acknowledged by the Kafka producer on a checkpoint.
	 */
	AT_LEAST_ONCE,
	/**
	 * Semantic.NONE means that nothing will be guaranteed. Messages can be lost and/or duplicated in case
	 * of failure.
	 */
	NONE
}
