/**
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
import java.util.Iterator;

import org.apache.flink.util.Collector;


public interface CoGroupFunction<V1, V2, O> extends Function, Serializable {
	
	/**
	 * This method must be implemented to provide a user implementation of a
	 * coGroup. It is called for each two key-value pairs that share the same
	 * key and come from different inputs.
	 * 
	 * @param first The records from the first input which were paired with the key.
	 * @param second The records from the second input which were paired with the key.
	 * @param out A collector that collects all output pairs.
	 */
	void coGroup(Iterator<V1> first, Iterator<V2> second, Collector<O> out) throws Exception;
	
}
