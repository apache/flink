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

package org.apache.flink.spargel.java.record;

import java.io.Serializable;

import org.apache.flink.api.common.aggregators.Aggregator;
import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Key;
import org.apache.flink.types.Record;
import org.apache.flink.types.Value;
import org.apache.flink.util.Collector;

/**
 * 
 * <VertexKey> The vertex key type.
 * <VertexValue> The vertex value type.
 * <Message> The message type.
 */
public abstract class VertexUpdateFunction<VertexKey extends Key<VertexKey>, VertexValue extends Value, Message extends Value> implements Serializable {

	// --------------------------------------------------------------------------------------------
	//  Public API Methods
	// --------------------------------------------------------------------------------------------
	
	public abstract void updateVertex(VertexKey vertexKey, VertexValue vertexValue, MessageIterator<Message> inMessages) throws Exception;
	
	public void setup(Configuration config) throws Exception {}
	
	public void preSuperstep() throws Exception {}
	
	public void postSuperstep() throws Exception {}
	
	public void setNewVertexValue(VertexValue newValue) {
		outVal.setField(1, newValue);
		out.collect(outVal);
	}
	
	public int getSuperstep() {
		return this.runtimeContext.getSuperstepNumber();
	}
	
	public <T extends Aggregator<?>> T getIterationAggregator(String name) {
		return this.runtimeContext.<T>getIterationAggregator(name);
	}
	
	public <T extends Value> T getPreviousIterationAggregate(String name) {
		return this.runtimeContext.<T>getPreviousIterationAggregate(name);
	}
	
	// --------------------------------------------------------------------------------------------
	//  internal methods
	// --------------------------------------------------------------------------------------------
	
	private IterationRuntimeContext runtimeContext;
	
	private Collector<Record> out;
	
	private Record outVal;
	
	
	void init(IterationRuntimeContext context) {
		this.runtimeContext = context;
	}
	
	void setOutput(Record val, Collector<Record> out) {
		this.out = out;
		this.outVal = val;
	}
	
	// serializability
	private static final long serialVersionUID = 1L;
}
