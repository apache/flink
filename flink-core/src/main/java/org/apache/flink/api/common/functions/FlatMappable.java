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

import org.apache.flink.util.Collector;

import java.io.Serializable;


/**
 *
 * @param <T>
 * @param <O>
 */
public interface FlatMappable<T, O> extends Function, Serializable {
	
	/**
	 * User defined function to perform transformations on records.
	 * This method allows to submit an arbitrary number of records
	 * per incoming tuple.
	 * 
	 * @param record incoming record
	 * @param out outgoing collector to return none, one or more records
	 * @throws Exception
	 */
	void flatMap(T record, Collector<O> out) throws Exception;
}
