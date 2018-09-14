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

package org.apache.flink.api.java.summarize.aggregation;

import java.lang.reflect.ParameterizedType;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * This harness uses multiple aggregators and variously aggregates and combines against
 * a list of values while calling a compareResults() method.
 *
 * <p>This method breaks the rule of "testing only one thing" by aggregating and combining
 * a bunch of different ways but can help uncover various kinds of bugs that can show
 * up in aggregators.
 *
 * @param <T> the type to aggregate
 * @param <R> the type of the results of the aggregation
 * @param <A> the aggregator to use
 */
public abstract class AggregateCombineHarness<T, R, A extends Aggregator<T, R>> {

	/**
	 * Compare results from different runs of aggregate/combine to make sure they are the same.
	 *
	 * <p>Subclasses should cause an Assertion failure or throw an Exception if the results aren't
	 * equal or at least close enough.
	 */
	protected abstract void compareResults(R result1, R result2);

	/**
	 * Variously aggregate and combine against a list of values, comparing results each time.
	 */
	@SafeVarargs
	public final R summarize(T... values) {
		if (values.length == 0) {
			// when there is nothing to aggregate just combine two empty aggregators and get the result.
			A agg1 = initAggregator();
			agg1.combine(initAggregator());
			return agg1.result();
		}
		else {
			R previousResult = null;
			R result = null;

			// Shuffling the values might cause test instability but only in the
			// case that there are underlying bugs that need to be fixed
			List<T> list = Arrays.asList(values);
			Collections.shuffle(list);

			for (int i = 0; i < values.length; i++) {

				// Two aggregators are used so that combine() can be tested also.
				// It shouldn't matter which aggregator is used because they are combined at the end so
				// we're looping through all points of the data and making sure it doesn't make a difference.

				A aggregator1 = initAggregator();
				A aggregator2 = initAggregator();

				for (int j = 0; j < i; j++) {
					aggregator1.aggregate(list.get(j));
				}
				for (int j = i; j < values.length; j++){
					aggregator2.aggregate(list.get(j));
				}

				aggregator1.combine(aggregator2);

				previousResult = result;
				result = aggregator1.result();

				if (previousResult != null) {
					// validate that variously aggregating then combining doesn't give different results
					compareResults(result, previousResult);
				}
			}
			return result;
		}
	}

	@SuppressWarnings("unchecked")
	private A initAggregator() {
		try {
			// Instantiate a generic type
			// http://stackoverflow.com/questions/75175/create-instance-of-generic-type-in-java
			return (A) ((Class) ((ParameterizedType) this.getClass().getGenericSuperclass()).getActualTypeArguments()[2]).newInstance();
		}
		catch (Exception e) {
			throw new RuntimeException("Could not initialize aggregator", e);
		}

	}

}
