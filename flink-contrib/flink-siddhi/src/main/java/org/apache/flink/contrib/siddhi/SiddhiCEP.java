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
import org.apache.flink.util.Preconditions;

import java.util.HashMap;
import java.util.Map;

/**
 * <p>
 * Siddhi CEP Environment, provides utility methods to
 *
 * <ul>
 *     <li>Initialize SiddhiCEP environment based on {@link StreamExecutionEnvironment}</li>
 *     <li>Register {@link SiddhiStream} with field-based StreamSchema and bind with physical source {@link DataStream}</li>
 *     <li>Define rich-featured Siddhi CEP execution plan with SQL-Like query for SiddhiStreamOperator</li>
 *     <li>Transform and connect source DataStream to SiddhiStreamOperator</li>
 *     <li>Register customizable siddhi plugins to extend built-in CEP functions</li>
 * </ul>
 * </p>
 *
 * @see SiddhiStream
 * @see org.apache.flink.contrib.siddhi.schema.StreamSchema
 * @see org.apache.flink.contrib.siddhi.operator.SiddhiStreamOperator
 */
@PublicEvolving
public class SiddhiCEP {
	private final StreamExecutionEnvironment executionEnvironment;
	private final Map<String, DataStream<?>> dataStreams = new HashMap<>();
	private final Map<String, SiddhiStreamSchema<?>> dataStreamSchemas = new HashMap<>();
	private final Map<String, Class<?>> extensions = new HashMap<>();

	/**
	 * @param streamExecutionEnvironment Stream Execution Environment
     */
	private SiddhiCEP(StreamExecutionEnvironment streamExecutionEnvironment) {
		this.executionEnvironment = streamExecutionEnvironment;
	}

	/**
	 * @see DataStream
	 * @return Siddhi streamId and source DataStream mapping.
     */
	public Map<String, DataStream<?>> getDataStreams() {
		return this.dataStreams;
	}

	/**
	 * @see SiddhiStreamSchema
	 * @return Siddhi streamId and stream schema mapping.
     */
	public Map<String, SiddhiStreamSchema<?>> getDataStreamSchemas() {
		return this.dataStreamSchemas;
	}

	/**
	 * @param streamId Siddhi streamId to check.
	 * @return whether the given streamId is defined in current SiddhiCEP environment.
     */
	public boolean isStreamDefined(String streamId) {
		Preconditions.checkNotNull(streamId,"streamId");
		return dataStreams.containsKey(streamId);
	}

	/**
	 * @return Registered siddhi extensions.
     */
	public Map<String, Class<?>> getExtensions() {
		return this.extensions;
	}

	/**
	 * Check whether given streamId has been defined, if not, throw {@link UndefinedStreamException}
	 * @param streamId Siddhi streamId to check.
	 * @throws UndefinedStreamException throws if given streamId is not defined
     */
	public void checkStreamDefined(String streamId) throws UndefinedStreamException {
		Preconditions.checkNotNull(streamId,"streamId");
		if (!isStreamDefined(streamId)) {
			throw new UndefinedStreamException("Stream (streamId: " + streamId + ") not defined");
		}
	}

	/**
	 * Define siddhi stream with streamId, source <code>DataStream</code> and stream schema,
	 * and select as initial source stream to connect to siddhi operator.
	 *
	 * @param streamId Unique siddhi streamId
	 * @param dataStream DataStream to bind to the siddhi stream.
	 * @param fieldNames Siddhi stream schema field names
	 *
	 * @see #registerStream(String, DataStream, String...)
	 * @see #from(String)
	 */
	public static <T> SiddhiStream.SingleSiddhiStream<T> define(String streamId, DataStream<T> dataStream, String... fieldNames) {
		Preconditions.checkNotNull(streamId,"streamId");
		Preconditions.checkNotNull(dataStream,"dataStream");
		Preconditions.checkNotNull(fieldNames,"fieldNames");
		SiddhiCEP environment = SiddhiCEP.getSiddhiEnvironment(dataStream.getExecutionEnvironment());
		return environment.from(streamId, dataStream, fieldNames);
	}

	/**
	 * Register stream with unique <code>streaId</code>, source <code>dataStream</code> and schema fields,
	 * and select the registered stream as initial stream to connect to Siddhi Runtime.
	 *
	 * @see #registerStream(String, DataStream, String...)
	 * @see #from(String)
     */
	public <T> SiddhiStream.SingleSiddhiStream<T> from(String streamId, DataStream<T> dataStream, String... fieldNames) {
		Preconditions.checkNotNull(streamId,"streamId");
		Preconditions.checkNotNull(dataStream,"dataStream");
		Preconditions.checkNotNull(fieldNames,"fieldNames");
		this.registerStream(streamId, dataStream, fieldNames);
		return new SiddhiStream.SingleSiddhiStream<>(streamId, this);
	}

	/**
	 * Select stream by streamId  as initial stream to connect to Siddhi Runtime.
	 *
	 * @param streamId Siddhi Stream Name
	 * @param <T> Stream Generic Type
     */
	public <T> SiddhiStream.SingleSiddhiStream<T> from(String streamId) {
		Preconditions.checkNotNull(streamId,"streamId");
		return new SiddhiStream.SingleSiddhiStream<>(streamId, this);
	}

	/**
	 * Select one stream and union other streams by streamId to connect to Siddhi Stream Operator.
	 *
	 * @param firstStreamId First siddhi streamId, which should be predefined in SiddhiCEP context.
	 * @param unionStreamIds Other siddhi streamIds to union, which should be predefined in SiddhiCEP context.
	 *
     * @return The UnionSiddhiStream Builder
     */
	public <T> SiddhiStream.UnionSiddhiStream<T> union(String firstStreamId, String... unionStreamIds) {
		Preconditions.checkNotNull(firstStreamId,"firstStreamId");
		Preconditions.checkNotNull(unionStreamIds,"unionStreamIds");
		return new SiddhiStream.SingleSiddhiStream<T>(firstStreamId, this).union(unionStreamIds);
	}

	/**
	 * Define siddhi stream with streamId, source <code>DataStream</code> and stream schema.
	 *
	 * @param streamId Unique siddhi streamId
	 * @param dataStream DataStream to bind to the siddhi stream.
	 * @param fieldNames Siddhi stream schema field names
     */
	public <T> void registerStream(final String streamId, DataStream<T> dataStream, String... fieldNames) {
		Preconditions.checkNotNull(streamId,"streamId");
		Preconditions.checkNotNull(dataStream,"dataStream");
		Preconditions.checkNotNull(fieldNames,"fieldNames");
		if (isStreamDefined(streamId)) {
			throw new DuplicatedStreamException("Input stream: " + streamId + " already exists");
		}
		dataStreams.put(streamId, dataStream);
		SiddhiStreamSchema<T> schema = new SiddhiStreamSchema<>(dataStream.getType(), fieldNames);
		schema.setTypeSerializer(schema.getTypeInfo().createSerializer(dataStream.getExecutionConfig()));
		dataStreamSchemas.put(streamId, schema);
	}

	/**
	 * @return Current StreamExecutionEnvironment.
     */
	public StreamExecutionEnvironment getExecutionEnvironment() {
		return executionEnvironment;
	}

	/**
	 * Register Siddhi CEP Extensions
	 *
	 * @see <a href="https://docs.wso2.com/display/CEP310/Writing+Extensions+to+Siddhi">https://docs.wso2.com/display/CEP310/Writing+Extensions+to+Siddhi</a>
	 * @param extensionName Unique siddhi extension name
	 * @param extensionClass Siddhi Extension class
     */
	public void registerExtension(String extensionName, Class<?> extensionClass) {
		if (extensions.containsKey(extensionName)) {
			throw new IllegalArgumentException("Extension named " + extensionName + " already registered");
		}
		extensions.put(extensionName, extensionClass);
	}

	/**
	 * Get registered source DataStream with Siddhi streamId.
	 *
	 * @param streamId Siddhi streamId
     * @return The source DataStream registered with Siddhi streamId
     */
	public <T> DataStream<T> getDataStream(String streamId) {
		if (this.dataStreams.containsKey(streamId)) {
			return (DataStream<T>) this.dataStreams.get(streamId);
		} else {
			throw new UndefinedStreamException("Undefined stream " + streamId);
		}
	}

	/**
	 * Create new SiddhiCEP instance.
	 *
	 * @param streamExecutionEnvironment StreamExecutionEnvironment
	 * @return New SiddhiCEP instance.
     */
	public static SiddhiCEP getSiddhiEnvironment(StreamExecutionEnvironment streamExecutionEnvironment) {
		return new SiddhiCEP(streamExecutionEnvironment);
	}
}
