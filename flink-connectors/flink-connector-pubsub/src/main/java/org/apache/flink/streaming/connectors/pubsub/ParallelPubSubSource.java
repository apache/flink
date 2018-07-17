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

package org.apache.flink.streaming.connectors.pubsub;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;


/**
 * PubSub Source, this Source will consume PubSub messages from a subscription and Acknowledge them as soon as they have been received.
 */
public class ParallelPubSubSource<OUT> extends PubSubSource<OUT> implements ParallelSourceFunction<OUT> {
	ParallelPubSubSource(SubscriberWrapper subscriberWrapper, DeserializationSchema<OUT> deserializationSchema, PubSubSourceBuilder.Mode mode) {
		super(subscriberWrapper, deserializationSchema, mode);
	}
}
