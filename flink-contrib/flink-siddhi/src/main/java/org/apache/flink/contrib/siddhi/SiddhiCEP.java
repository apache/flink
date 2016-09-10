/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.siddhi;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.contrib.siddhi.exception.DuplicatedStreamException;
import org.apache.flink.contrib.siddhi.exception.UndefinedStreamException;
import org.apache.flink.contrib.siddhi.schema.SiddhiStreamSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.HashMap;
import java.util.Map;

/**
 * Siddhi CEP Execution Environment
 */
@PublicEvolving
public class SiddhiCEP {
	private final StreamExecutionEnvironment executionEnvironment;
	private final Map<String, DataStream<?>> dataStreams = new HashMap<>();
	private final Map<String, SiddhiStreamSchema<?>> dataStreamSchemas = new HashMap<>();
	private final Map<String, Class<?>> extensions = new HashMap<>();

	public Map<String, DataStream<?>> getDataStreams() {
		return this.dataStreams;
	}

	public Map<String, SiddhiStreamSchema<?>> getDataStreamSchemas() {
		return this.dataStreamSchemas;
	}

	public boolean isStreamDefined(String streamId) {
		return dataStreams.containsKey(streamId);
	}

	public Map<String, Class<?>> getExtensions() {
		return this.extensions;
	}

	public void checkStreamDefined(String streamId) throws UndefinedStreamException {
		if (!isStreamDefined(streamId)) {
			throw new UndefinedStreamException("Stream (streamId: " + streamId + ") not defined");
		}
	}

	public SiddhiCEP(StreamExecutionEnvironment streamExecutionEnvironment) {
		this.executionEnvironment = streamExecutionEnvironment;
	}

	public static <T> SiddhiStream.SingleSiddhiStream<T> define(String streamId, DataStream<T> inStream, String... fieldNames) {
		SiddhiCEP environment = SiddhiCEP.getSiddhiEnvironment(inStream.getExecutionEnvironment());
		return environment.from(streamId, inStream, fieldNames);
	}

	public <T> SiddhiStream.SingleSiddhiStream<T> from(String streamId, DataStream<T> inStream, String... fieldNames) {
		this.registerStream(streamId, inStream, fieldNames);
		return new SiddhiStream.SingleSiddhiStream<>(streamId, this);
	}

	public <T> SiddhiStream.SingleSiddhiStream<T> from(String streamId) {
		return new SiddhiStream.SingleSiddhiStream<>(streamId, this);
	}

	public <T> SiddhiStream.UnionSiddhiStream<T> union(String firstStreamId, String... unionStreamIds) {
		return new SiddhiStream.SingleSiddhiStream<T>(firstStreamId, this).union(unionStreamIds);
	}

	public <T> void registerStream(final String streamId, DataStream<T> dataStream, String... fieldNames) {
		if (isStreamDefined(streamId)) {
			throw new DuplicatedStreamException("Input stream: " + streamId + " already exists");
		}
		dataStreams.put(streamId, dataStream);
		SiddhiStreamSchema<T> schema = new SiddhiStreamSchema<>(dataStream.getType(), fieldNames);
		schema.setTypeSerializer(schema.getTypeInfo().createSerializer(dataStream.getExecutionConfig()));
		dataStreamSchemas.put(streamId, schema);
	}

	public StreamExecutionEnvironment getExecutionEnvironment() {
		return executionEnvironment;
	}

	public void registerExtension(String extensionName, Class<?> extensionClass) {
		if (extensions.containsKey(extensionName)) {
			throw new IllegalArgumentException("Extension named " + extensionName + " already registered");
		}
		extensions.put(extensionName, extensionClass);
	}

	public <T> DataStream<T> getDataStream(String streamId) {
		if (this.dataStreams.containsKey(streamId)) {
			return (DataStream<T>) this.dataStreams.get(streamId);
		} else {
			throw new UndefinedStreamException("Undefined stream " + streamId);
		}
	}

	public static SiddhiCEP getSiddhiEnvironment(StreamExecutionEnvironment streamExecutionEnvironment) {
		return new SiddhiCEP(streamExecutionEnvironment);
	}
}
