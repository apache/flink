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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.contrib.siddhi.schema.SiddhiStreamSchema;
import org.apache.flink.contrib.siddhi.schema.StreamSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * SiddhiCEP Operator Context
 */
public class SiddhiOperatorContext implements Serializable {
	private Map<String, SiddhiStreamSchema<?>> inputStreamSchemas;
	private String outputStreamId;
	private TypeInformation outputStreamType;
	private TimeCharacteristic timeCharacteristic;
	private String name;

	private String executionExpression;

	public SiddhiOperatorContext() {
		inputStreamSchemas = new HashMap<>();
	}

	public String getName() {
		if (this.name == null) {
			if (executionExpression.length() > 50) {
				return "Siddhi: " + executionExpression.substring(0, 50) + " ...";
			} else {
				return "Siddhi: " + executionExpression;
			}
		} else {
			return this.name;
		}
	}

	public List<String> getInputStreams() {
		Object[] keys = this.inputStreamSchemas.keySet().toArray();
		List<String> result = new ArrayList<>(keys.length);
		for (Object key : keys) {
			result.add((String) key);
		}
		return result;
	}

	public String getExecutionExpression() {
		return executionExpression;
	}

	/**
	 * Stream definition + execution expression
	 */
	public String getFinalExecutionExpression() {
		StringBuilder sb = new StringBuilder();
		for (Map.Entry<String, SiddhiStreamSchema<?>> entry : inputStreamSchemas.entrySet()) {
			sb.append(entry.getValue().getStreamDefinitionExpression(entry.getKey()));
		}
		sb.append(this.getExecutionExpression());
		return sb.toString();
	}

	public TypeInformation getOutputStreamType() {
		return outputStreamType;
	}

	public String getOutputStreamId() {
		return outputStreamId;
	}

	@SuppressWarnings("unchecked")
	public <IN> StreamSchema<IN> getInputStreamSchema(String inputStreamId) {
		if (!inputStreamSchemas.containsKey(inputStreamId)) {
			throw new IllegalArgumentException("Input stream: " + inputStreamId + " is not found");
		}
		return (StreamSchema<IN>) inputStreamSchemas.get(inputStreamId);
	}

	public void setOutputStreamId(String outputStreamId) {
		this.outputStreamId = outputStreamId;
	}

	public void setOutputStreamType(TypeInformation outputStreamType) {
		this.outputStreamType = outputStreamType;
	}

	public TimeCharacteristic getTimeCharacteristic() {
		return timeCharacteristic;
	}

	public void setTimeCharacteristic(TimeCharacteristic timeCharacteristic) {
		this.timeCharacteristic = timeCharacteristic;
	}

	public void setExecutionExpression(String executionExpression) {
		this.executionExpression = executionExpression;
	}

	public Map<String, SiddhiStreamSchema<?>> getInputStreamSchemas() {
		return inputStreamSchemas;
	}

	public void setInputStreamSchemas(Map<String, SiddhiStreamSchema<?>> inputStreamSchemas) {
		this.inputStreamSchemas = inputStreamSchemas;
	}

	public void setName(String name) {
		this.name = name;
	}
}
