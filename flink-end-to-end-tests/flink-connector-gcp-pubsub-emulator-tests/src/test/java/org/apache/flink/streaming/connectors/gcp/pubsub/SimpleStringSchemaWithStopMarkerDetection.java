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

package org.apache.flink.streaming.connectors.gcp.pubsub;

import org.apache.flink.api.common.serialization.SimpleStringSchema;

/**
 * <p>For testing PubSub we need to have messages that can indicate the end of the stream
 * to cleanly terminate the test run.</p>
 *
 * <p>IMPORTANT NOTE: This way of testing uses an effect of the PubSub emulator that is absolutely
 * guaranteed NOT to work in the real PubSub: The ordering of the messages is maintained in the topic.
 * So here we can assume that if we add a stop message LAST we can terminate the test stream when we see it.</p>
 */
public class SimpleStringSchemaWithStopMarkerDetection extends SimpleStringSchema {
	public static final String STOP_MARKER = "<><><>STOP STOP STOP<><><>";

	@Override
	public boolean isEndOfStream(String s) {
		return (STOP_MARKER.equals(s));
	}
}
