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

import org.apache.flink.annotation.Public;
import org.apache.flink.util.Collector;

import java.io.Serializable;

/**
 * Interface for "mapPartition" functions. A "mapPartition" function is called a single time per
 * data partition receives an Iterable with data elements of that partition. It may return an
 * arbitrary number of data elements.
 *
 * <p>This function is intended to provide enhanced flexibility in the processing of elements in a partition.
 * For most of the simple use cases, consider using the {@link MapFunction} or {@link FlatMapFunction}.
 *
 * <p>The basic syntax for a MapPartitionFunction is as follows:
 * <pre>{@code
 * DataSet<X> input = ...;
 *
 * DataSet<Y> result = input.mapPartition(new MyMapPartitionFunction());
 * }</pre>
 *
 * @param <T> Type of the input elements.
 * @param <O> Type of the returned elements.
 */
@Public
@FunctionalInterface
public interface MapPartitionFunction<T, O> extends Function, Serializable {

	/**
	 * A user-implemented function that modifies or transforms an incoming object.
	 *
	 * @param values All records for the mapper
	 * @param out The collector to hand results to.
	 * @throws Exception This method may throw exceptions. Throwing an exception will cause the operation
	 *                   to fail and may trigger recovery.
	 */
	void mapPartition(Iterable<T> values, Collector<O> out) throws Exception;
}
