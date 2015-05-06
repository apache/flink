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

package org.apache.flink.graph.gsa;

import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;

import java.io.Serializable;

@SuppressWarnings("serial")
public abstract class ApplyFunction<K, VV, M> implements Serializable {

	public abstract void apply(M newValue, VV currentValue);

	/**
	 * Sets the result for the apply function
	 *
	 * @param result the result of the apply phase
	 */
	public void setResult(VV result) {
		outVal.f1 = result;
		out.collect(outVal);
	}

	/**
	 * This method is executed once per superstep before the vertex update function is invoked for each vertex.
	 *
	 * @throws Exception Exceptions in the pre-superstep phase cause the superstep to fail.
	 */
	public void preSuperstep() {}

	/**
	 * This method is executed once per superstep after the vertex update function has been invoked for each vertex.
	 *
	 * @throws Exception Exceptions in the post-superstep phase cause the superstep to fail.
	 */
	public void postSuperstep() {}

	/**
	 * Gets the number of the superstep, starting at <tt>1</tt>.
	 *
	 * @return The number of the current superstep.
	 */
	public int getSuperstepNumber() {
		return this.runtimeContext.getSuperstepNumber();
	}

	// --------------------------------------------------------------------------------------------
	//  Internal methods
	// --------------------------------------------------------------------------------------------

	private IterationRuntimeContext runtimeContext;

	private Collector<Vertex<K, VV>> out;

	private Vertex<K, VV> outVal;

	public void init(IterationRuntimeContext iterationRuntimeContext) {
		this.runtimeContext = iterationRuntimeContext;
	}

	public void setOutput(Vertex<K, VV> vertex, Collector<Vertex<K, VV>> out) {
		this.out = out;
		this.outVal = vertex;
	}

}
