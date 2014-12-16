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

package org.apache.flink.api.java.aggregation;

import static java.lang.String.format;

import org.apache.commons.lang3.Validate;
import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;

/**
 * Base class for the {@link MapFunction}'s and {@link ReduceFunction}
 * used during an aggregation implementing the construction of an output
 * tuple of a given arity.
 * 
 * @param <OUT> The output tuple.
 */
public class AggregationUdfBase<OUT extends Tuple> extends AbstractRichFunction {
	private static final long serialVersionUID = -7383673230236467603L;

	private int arity;
	
	public AggregationUdfBase(int arity) {
		this.arity = arity;
		Validate.inclusiveBetween(1, Tuple.MAX_ARITY, arity, 
				format("The arity of the intermediate tuple must be between {0} and {1}", 1, Tuple.MAX_ARITY));
	}
	
	@SuppressWarnings("unchecked")
	OUT createResultTuple() {
		String resultTupleClassName = "org.apache.flink.api.java.tuple.Tuple" + String.valueOf(arity);
		OUT result = null;
		try {
			result = (OUT) Class.forName(resultTupleClassName).newInstance();
		} catch (InstantiationException e) {
			throw new IllegalArgumentException("Could not create output tuple", e);
		} catch (IllegalAccessException e) {
			throw new IllegalArgumentException("Could not create output tuple", e);
		} catch (ClassNotFoundException e) {
			throw new IllegalArgumentException("Could not create output tuple", e);
		}
		return result;
	}
	
}
