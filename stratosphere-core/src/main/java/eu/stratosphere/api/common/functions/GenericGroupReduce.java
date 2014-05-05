/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.api.common.functions;

import java.util.Iterator;

import eu.stratosphere.util.Collector;


/**
 *
 * @param <T> Incoming types
 * @param <O> Outgoing types
 */
public interface GenericGroupReduce<T, O> extends Function {
	/**
	 * 
	 * The central function to be implemented for a reducer. The function receives per call one
	 * key and all the values that belong to that key. Each key is guaranteed to be processed by exactly
	 * one function call across all involved instances across all computing nodes.
	 * 
	 * @param records All records that belong to the given input key.
	 * @param out The collector to hand results to.
	 * @throws Exception
	 */
	void reduce(Iterator<T> records, Collector<O> out) throws Exception;
}
