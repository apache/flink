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

package org.apache.flink.cep.pattern.conditions;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.cep.time.TimeContext;

import java.io.Serializable;

/**
 * A user-defined condition that decides if an element should be accepted in the pattern or not.
 * Accepting an element also signals a state transition for the corresponding {@link
 * org.apache.flink.cep.nfa.NFA}.
 *
 * <p>A condition can be a simple filter or a more complex condition that iterates over the
 * previously accepted elements in the pattern and decides to accept a new element or not based on
 * some statistic over these elements. In the former case, the condition should extend the {@link
 * SimpleCondition} class. In the later, the condition should extend this class, which gives you
 * also access to the previously accepted elements through a {@link Context}.
 *
 * <p>An iterative condition that accepts an element if i) its name is middle, and ii) the sum of
 * the prices of all accepted elements is less than {@code 5} would look like:
 *
 * <pre>{@code
 * private class MyCondition extends IterativeCondition<Event> {
 *
 * 		@Override
 *     	public boolean filter(Event value, Context<Event> ctx) throws Exception {
 *     		if (!value.getName().equals("middle")) {
 *     			return false;
 *     		}
 *
 *     		double sum = 0.0;
 *     		for (Event e: ctx.getEventsForPattern("middle")) {
 *     			sum += e.getPrice();
 *     		}
 *     		sum += value.getPrice();
 *     		return Double.compare(sum, 5.0) <= 0;
 *     	}
 *    }
 * }</pre>
 *
 * <b>ATTENTION: </b> The call to {@link Context#getEventsForPattern(String)
 * getEventsForPattern(...)} has to find the elements that belong to the pattern among the elements
 * stored by the NFA. The cost of this operation can vary, so when implementing your condition, try
 * to minimize the times the method is called.
 */
@PublicEvolving
public abstract class IterativeCondition<T> implements Function, Serializable {

    private static final long serialVersionUID = 7067817235759351255L;

    /**
     * The filter function that evaluates the predicate.
     *
     * <p><strong>IMPORTANT:</strong> The system assumes that the function does not modify the
     * elements on which the predicate is applied. Violating this assumption can lead to incorrect
     * results.
     *
     * @param value The value to be tested.
     * @param ctx The {@link Context} used for the evaluation of the function and provides access to
     *     the already accepted events in the pattern (see {@link
     *     Context#getEventsForPattern(String)}).
     * @return {@code true} for values that should be retained, {@code false} for values to be
     *     filtered out.
     * @throws Exception This method may throw exceptions. Throwing an exception will cause the
     *     operation to fail and may trigger recovery.
     */
    public abstract boolean filter(T value, Context<T> ctx) throws Exception;

    /** The context used when evaluating the {@link IterativeCondition condition}. */
    public interface Context<T> extends TimeContext {

        /**
         * @return An {@link Iterable} over the already accepted elements for a given pattern.
         *     Elements are iterated in the order they were inserted in the pattern.
         * @param name The name of the pattern.
         */
        Iterable<T> getEventsForPattern(String name) throws Exception;

        <ACC> ACC getAccumulator(String stateKey, TypeSerializer<ACC> serializer) throws Exception;

        <ACC> void putAccumulator(String stateKey, ACC accumulator, TypeSerializer<ACC> serializer)
                throws Exception;
    }
}
