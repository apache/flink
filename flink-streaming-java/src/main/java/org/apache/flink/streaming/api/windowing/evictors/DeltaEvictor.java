/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.windowing.evictors;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.api.functions.windowing.delta.DeltaFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;

import org.apache.flink.shaded.guava18.com.google.common.collect.Iterables;

import java.util.Iterator;

/**
 * An {@link Evictor} that keeps elements based on a {@link DeltaFunction} and a threshold.
 *
 * <p>Eviction starts from the first element of the buffer and removes all elements from the buffer
 * which have a higher delta then the threshold.
 *
 * @param <W> The type of {@link Window Windows} on which this {@code Evictor} can operate.
 */
@PublicEvolving
public class DeltaEvictor<T, W extends Window> implements Evictor<T, W> {
    private static final long serialVersionUID = 1L;

    DeltaFunction<T> deltaFunction;
    private double threshold;
    private final boolean doEvictAfter;

    private DeltaEvictor(double threshold, DeltaFunction<T> deltaFunction) {
        this.deltaFunction = deltaFunction;
        this.threshold = threshold;
        this.doEvictAfter = false;
    }

    private DeltaEvictor(double threshold, DeltaFunction<T> deltaFunction, boolean doEvictAfter) {
        this.deltaFunction = deltaFunction;
        this.threshold = threshold;
        this.doEvictAfter = doEvictAfter;
    }

    @Override
    public void evictBefore(
            Iterable<TimestampedValue<T>> elements, int size, W window, EvictorContext ctx) {
        if (!doEvictAfter) {
            evict(elements, size, ctx);
        }
    }

    @Override
    public void evictAfter(
            Iterable<TimestampedValue<T>> elements, int size, W window, EvictorContext ctx) {
        if (doEvictAfter) {
            evict(elements, size, ctx);
        }
    }

    private void evict(Iterable<TimestampedValue<T>> elements, int size, EvictorContext ctx) {
        TimestampedValue<T> lastElement = Iterables.getLast(elements);
        for (Iterator<TimestampedValue<T>> iterator = elements.iterator(); iterator.hasNext(); ) {
            TimestampedValue<T> element = iterator.next();
            if (deltaFunction.getDelta(element.getValue(), lastElement.getValue())
                    >= this.threshold) {
                iterator.remove();
            }
        }
    }

    @Override
    public String toString() {
        return "DeltaEvictor(" + deltaFunction + ", " + threshold + ")";
    }

    /**
     * Creates a {@code DeltaEvictor} from the given threshold and {@code DeltaFunction}. Eviction
     * is done before the window function.
     *
     * @param threshold The threshold
     * @param deltaFunction The {@code DeltaFunction}
     */
    public static <T, W extends Window> DeltaEvictor<T, W> of(
            double threshold, DeltaFunction<T> deltaFunction) {
        return new DeltaEvictor<>(threshold, deltaFunction);
    }

    /**
     * Creates a {@code DeltaEvictor} from the given threshold, {@code DeltaFunction}. Eviction is
     * done before/after the window function based on the value of doEvictAfter.
     *
     * @param threshold The threshold
     * @param deltaFunction The {@code DeltaFunction}
     * @param doEvictAfter Whether eviction should be done after window function
     */
    public static <T, W extends Window> DeltaEvictor<T, W> of(
            double threshold, DeltaFunction<T> deltaFunction, boolean doEvictAfter) {
        return new DeltaEvictor<>(threshold, deltaFunction, doEvictAfter);
    }
}
