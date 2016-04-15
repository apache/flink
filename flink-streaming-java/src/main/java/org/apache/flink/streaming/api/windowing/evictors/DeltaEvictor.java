/**
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

import com.google.common.collect.Iterables;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.api.functions.windowing.delta.DeltaFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * An {@link Evictor} that keeps elements based on a {@link DeltaFunction} and a threshold.
 *
 * <p>
 * Eviction starts from the first element of the buffer and removes all elements from the buffer
 * which have a higher delta then the threshold. As soon as there is an element with a lower delta,
 * the eviction stops.
 *
 * @param <W> The type of {@link Window Windows} on which this {@code Evictor} can operate.
 */
@PublicEvolving
public class DeltaEvictor<T, W extends Window> implements Evictor<T, W> {
	private static final long serialVersionUID = 1L;

	DeltaFunction<T> deltaFunction;
	private double threshold;

	private DeltaEvictor(double threshold, DeltaFunction<T> deltaFunction) {
		this.deltaFunction = deltaFunction;
		this.threshold = threshold;
	}

	@Override
	public int evict(Iterable<StreamRecord<T>> elements, int size, W window) {
		StreamRecord<T> lastElement = Iterables.getLast(elements);
		int toEvict = 0;
		for (StreamRecord<T> element : elements) {
			if (deltaFunction.getDelta(element.getValue(), lastElement.getValue()) < this.threshold) {
				break;
			}
			toEvict++;
		}

		return toEvict;
	}

	@Override
	public String toString() {
		return "DeltaEvictor(" +  deltaFunction + ", " + threshold + ")";
	}

	/**
	 * Creates a {@code DeltaEvictor} from the given threshold and {@code DeltaFunction}.
	 *
	 * @param threshold The threshold
	 * @param deltaFunction The {@code DeltaFunction}
	 */
	public static <T, W extends Window> DeltaEvictor<T, W> of(double threshold, DeltaFunction<T> deltaFunction) {
		return new DeltaEvictor<>(threshold, deltaFunction);
	}
}
