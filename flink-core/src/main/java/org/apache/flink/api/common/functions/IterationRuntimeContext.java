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

package org.apache.flink.api.common.functions;

import java.io.Serializable;

import org.apache.flink.api.common.accumulators.Accumulator;

/**
 *
 */
public interface IterationRuntimeContext extends RuntimeContext {
	
	/**
	 * Gets the number of the current superstep. Superstep numbers start at <i>1</i>.
	 * 
	 * @return The number of the current superstep.
	 */
	int getSuperstepNumber();
	
	/**
	 * Adds an accumulator to this iteration. The accumulator is reset after each superstep.
	 * 
	 * @param name
	 * @param accumulator
	 */
	<V, A extends Serializable> void addIterationAccumulator(String name, Accumulator<V, A> accumulator);
	
	/**
	 * Returns the accumulated value of the last iteration
	 * 
	 * @param name
	 * @return
	 */
	<T extends Accumulator<?, ? extends Serializable>> T getPreviousIterationAccumulator(String name);
}