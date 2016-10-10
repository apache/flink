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

package org.apache.flink.contrib.siddhi.operator;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.contrib.siddhi.exception.UndefinedStreamException;
import org.apache.flink.contrib.siddhi.schema.SiddhiStreamSchema;
import org.apache.flink.contrib.siddhi.schema.StreamSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.util.Preconditions;
import org.wso2.siddhi.core.SiddhiManager;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * SiddhiCEP Operator Context Metadata including input/output stream (streamId, TypeInformation) as well execution plan query,
 * and execution environment context like TimeCharacteristic and ExecutionConfig.
 */
public class SiddhiOperatorContext implements Serializable {
	private ExecutionConfig executionConfig;
	private Map<String, SiddhiStreamSchema<?>> inputStreamSchemas;
	private final Map<String, Class<?>> siddhiExtensions;
	private String outputStreamId;
	private TypeInformation outputStreamType;
	private TimeCharacteristic timeCharacteristic;
	private String name;
	private String executionPlan;

	public SiddhiOperatorContext() {
		inputStreamSchemas = new HashMap<>();
		siddhiExtensions = new HashMap<>();
	}

	/**
	 * @param extensions siddhi extensions to register
     */
	public void setExtensions(Map<String, Class<?>> extensions) {
		Preconditions.checkNotNull(extensions,"extensions");
		siddhiExtensions.putAll(extensions);
	}

	/**
	 * @return registered siddhi extensions
     */
	public Map<String, Class<?>> getExtensions() {
		return siddhiExtensions;
	}

	/**
	 * @return Siddhi Stream Operator Name in format of "Siddhi: execution query ... (query length)"
     */
	public String getName() {
		if (this.name == null) {
			if (executionPlan.length() > 100) {
				return String.format("Siddhi: %s ... (%s)", executionPlan.substring(0, 100), executionPlan.length() - 100);
			} else {
				return String.format("Siddhi: %s", executionPlan);
			}
		} else {
			return this.name;
		}
	}

	/**
	 * @return Source siddhi stream IDs
     */
	public List<String> getInputStreams() {
		Object[] keys = this.inputStreamSchemas.keySet().toArray();
		List<String> result = new ArrayList<>(keys.length);
		for (Object key : keys) {
			result.add((String) key);
		}
		return result;
	}

	/**
	 * @return Siddhi CEP cql-like execution plan
     */
	public String getExecutionPlan() {
		return executionPlan;
	}

	/**
	 * Stream definition + execution expression
	 */
	public String getFinalExecutionPlan() {
		Preconditions.checkNotNull(executionPlan, "Execution plan is not set");
		StringBuilder sb = new StringBuilder();
		for (Map.Entry<String, SiddhiStreamSchema<?>> entry : inputStreamSchemas.entrySet()) {
			sb.append(entry.getValue().getStreamDefinitionExpression(entry.getKey()));
		}
		sb.append(this.getExecutionPlan());
		return sb.toString();
	}

	/**
	 * @return Siddhi Stream Operator output type information
     */
	public TypeInformation getOutputStreamType() {
		return outputStreamType;
	}

	/**
	 * @return Siddhi output streamId for callback
     */
	public String getOutputStreamId() {
		return outputStreamId;
	}

	/**
	 * @param inputStreamId Siddhi streamId
     * @return StreamSchema for given siddhi streamId
	 *
	 * @throws UndefinedStreamException throws if stream is not defined
     */
	@SuppressWarnings("unchecked")
	public <IN> StreamSchema<IN> getInputStreamSchema(String inputStreamId) {
		Preconditions.checkNotNull(inputStreamId,"inputStreamId");

		if (!inputStreamSchemas.containsKey(inputStreamId)) {
			throw new UndefinedStreamException("Input stream: " + inputStreamId + " is not found");
		}
		return (StreamSchema<IN>) inputStreamSchemas.get(inputStreamId);
	}

	/**
	 * @param outputStreamId Siddhi output streamId, which must exist in siddhi execution plan
     */
	public void setOutputStreamId(String outputStreamId) {
		Preconditions.checkNotNull(outputStreamId,"outputStreamId");
		this.outputStreamId = outputStreamId;
	}

	/**
	 * @param outputStreamType Output stream TypeInformation
     */
	public void setOutputStreamType(TypeInformation outputStreamType) {
		Preconditions.checkNotNull(outputStreamType,"outputStreamType");
		this.outputStreamType = outputStreamType;
	}

	/**
	 * @return Returns execution environment TimeCharacteristic
     */
	public TimeCharacteristic getTimeCharacteristic() {
		return timeCharacteristic;
	}

	public void setTimeCharacteristic(TimeCharacteristic timeCharacteristic) {
		Preconditions.checkNotNull(timeCharacteristic,"timeCharacteristic");
		this.timeCharacteristic = timeCharacteristic;
	}

	/**
	 * @param executionPlan Siddhi SQL-Like exeuction plan query
     */
	public void setExecutionPlan(String executionPlan) {
		Preconditions.checkNotNull(executionPlan,"executionPlan");
		this.executionPlan = executionPlan;
	}

	/**
	 * @return Returns input stream ID and  schema mapping
     */
	public Map<String, SiddhiStreamSchema<?>> getInputStreamSchemas() {
		return inputStreamSchemas;
	}

	/**
	 * @param inputStreamSchemas input stream ID and  schema mapping
     */
	public void setInputStreamSchemas(Map<String, SiddhiStreamSchema<?>> inputStreamSchemas) {
		Preconditions.checkNotNull(inputStreamSchemas,"inputStreamSchemas");
		this.inputStreamSchemas = inputStreamSchemas;
	}

	public void setName(String name) {
		Preconditions.checkNotNull(name,"name");
		this.name = name;
	}

	/**
	 * @return Created new SiddhiManager instance with registered siddhi extensions
     */
	public SiddhiManager createSiddhiManager() {
		SiddhiManager siddhiManager = new SiddhiManager();
		for (Map.Entry<String, Class<?>> entry : getExtensions().entrySet()) {
			siddhiManager.setExtension(entry.getKey(), entry.getValue());
		}
		return siddhiManager;
	}

	/**
	 * @return StreamExecutionEnvironment ExecutionConfig
     */
	public ExecutionConfig getExecutionConfig() {
		return executionConfig;
	}

	/**
	 * @param executionConfig StreamExecutionEnvironment ExecutionConfig
     */
	public void setExecutionConfig(ExecutionConfig executionConfig) {
		Preconditions.checkNotNull(executionConfig,"executionConfig");
		this.executionConfig = executionConfig;
	}
}
