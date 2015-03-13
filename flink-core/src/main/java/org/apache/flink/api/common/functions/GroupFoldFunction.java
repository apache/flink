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

import org.apache.flink.util.Collector;

/**
 * The interface for group fold functions. GroupFoldFunctions process groups of elements.
 * They may aggregate them to a single value, or produce multiple result values for each group.
 * The group may be defined by sharing a common grouping key, or the group may simply be
 * all elements of a data set.
 * <p>
 * For a fold function that works incrementally by combining always two elements, see
 * {@link FoldFunction}.
 * <p>
 * The basic syntax for using a grouped GroupFoldFunction is as follows:
 * <pre><blockquote>
 * DataSet<X> input = ...;
 *
 * X initialValue = ...;
 * DataSet<X> result = input.groupBy(<key-definition>).foldGroup(new MyGroupFoldFunction(), initialValue);
 * </blockquote></pre>
 *
 * @param <T> Type of the initial input and the returned element
 * @param <O> Type of the elements that the group/list/stream contains
 */
public interface GroupFoldFunction<T, O> extends Function, Serializable {
	/**
	 * The fold method. The function receives one call per group of elements.
	 *
	 * @param values All records that belong to the given input key.
	 * @param out The collector to hand results to.
	 *
	 * @throws Exception This method may throw exceptions. Throwing an exception will cause the operation
	 *                   to fail and may trigger recovery.
	 */
	void fold(Iterable<T> values, Collector<O> out) throws Exception;
}
