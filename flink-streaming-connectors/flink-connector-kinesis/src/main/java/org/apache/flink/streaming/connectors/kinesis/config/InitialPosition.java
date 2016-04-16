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

package org.apache.flink.streaming.connectors.kinesis.config;

import com.amazonaws.services.kinesis.model.ShardIteratorType;

/**
 * The initial position to start reading shards from. This will affect the {@link ShardIteratorType} used
 * when the consumer tasks retrieve the first shard iterator for each Kinesis shard.
 */
public enum InitialPosition {

	/** Start reading from the earliest possible record in the stream (excluding expired data records) */
	TRIM_HORIZON,

	/** Start reading from the latest incoming record */
	LATEST
}
