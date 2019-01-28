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

package org.apache.flink.addons.hbase;

import org.apache.flink.api.java.tuple.Tuple;

import org.apache.hadoop.hbase.client.Result;

/**
 * HBase scanned {@link Result} to {@link Tuple} mapper.
 */
public interface ResultToTupleMapper<T extends Tuple> {

	/**
	 * The output from HBase is always an instance of {@link Result}.
	 * This method is to copy the data in the Result instance into the required {@link Tuple}
	 * @param r The Result instance from HBase that needs to be converted
	 * @return The appropriate instance of {@link Tuple} that contains the needed information.
	 */
	T mapResultToTuple(Result r);
}
