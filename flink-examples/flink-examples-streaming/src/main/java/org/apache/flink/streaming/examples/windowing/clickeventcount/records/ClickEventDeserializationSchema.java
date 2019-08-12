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

package org.apache.flink.streaming.examples.windowing.clickeventcount.records;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

/**
 * A Kafka {@link DeserializationSchema} to deserialize {@link ClickEvent}s from JSON.
 *
 */
public class ClickEventDeserializationSchema implements DeserializationSchema<ClickEvent> {

	private static final long serialVersionUID = 1L;

	private static final ObjectMapper objectMapper = new ObjectMapper();

	@Override
	public ClickEvent deserialize(byte[] message) throws IOException {
		return objectMapper.readValue(message, ClickEvent.class);
	}

	@Override
	public boolean isEndOfStream(ClickEvent nextElement) {
		return false;
	}

	@Override
	public TypeInformation<ClickEvent> getProducedType() {
		return TypeInformation.of(ClickEvent.class);
	}
}
