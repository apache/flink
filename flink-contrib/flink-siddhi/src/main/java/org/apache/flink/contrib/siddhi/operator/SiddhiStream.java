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

import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.contrib.siddhi.schema.SiddhiStreamSchema;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.HashMap;
import java.util.Map;

/**
 * Non-serializable
 */
public class SiddhiStream<OUT> {
	private final Map<String,DataStream<?>> inputStreams;
	private final SiddhiExecutionPlan<OUT> siddhiExecutionPlan;
	private String name;

	public SiddhiStream() {
		this.inputStreams = new HashMap<>();
		this.siddhiExecutionPlan = new SiddhiExecutionPlan<>();
	}

	public String getName(){
		if(this.name == null) {
			if (siddhiExecutionPlan.getExecutionExpression().length() > 50) {
				return "Siddhi: " + siddhiExecutionPlan.getExecutionExpression().substring(0, 50) + " ...";
			} else {
				return "Siddhi: " + siddhiExecutionPlan.getExecutionExpression();
			}
		} else {
			return this.name;
		}
	}

	public <T> void registerInput(String streamId, DataStream<T> inStream,String ... fields) {
		if(inputStreams.containsKey(streamId)){
			throw new IllegalArgumentException("Input stream: "+streamId+" already exists");
		}
		inputStreams.put(streamId,inStream);
		SiddhiStreamSchema<T> schema = new SiddhiStreamSchema<>(inStream.getType(), fields);
		schema.setTypeSerializer(schema.getTypeInfo().createSerializer(inStream.getExecutionConfig()));
		siddhiExecutionPlan.inputStreamSchemas.put(streamId, schema);

		if(this.siddhiExecutionPlan.getTimeCharacteristic() == null){
			this.siddhiExecutionPlan.setTimeCharacteristic(inStream.getExecutionEnvironment().getStreamTimeCharacteristic());
		}
	}

	public void setOutput(String streamId, Class<OUT> type) {
		if(this.siddhiExecutionPlan.getOutputStreamId()!=null){
			throw new IllegalArgumentException("Output stream: "+streamId+" was already set");
		}
		siddhiExecutionPlan.setOutputStreamId(streamId);
		siddhiExecutionPlan.setOutputStreamType(TypeExtractor.createTypeInfo(type));
	}

	public SiddhiExecutionPlan<OUT> getSiddhiExecutionPlan(){
		return this.siddhiExecutionPlan;
	}

	public Map<String,DataStream<?>> getInputStreams(){
		return this.inputStreams;
	}

	public void setExecutionExpression(String executionExpression) {
		// TODO: Validate execution expression
		siddhiExecutionPlan.executionExpression = executionExpression;
	}

	public void setName(String name) {
		this.name = name;
	}
}
