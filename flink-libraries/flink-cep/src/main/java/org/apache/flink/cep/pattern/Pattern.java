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

package org.apache.flink.cep.pattern;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.streaming.api.windowing.time.Time;

public class Pattern<T, F extends T> {

	private final String name;
	private final Pattern<T, ?> previous;
	private FilterFunction<F> filterFunction;
	private Time windowTime;

	protected Pattern(final String name, final Pattern<T, ?> previous) {
		this.name = name;
		this.previous = previous;
	}

	public String getName() {
		return name;
	}

	public Pattern<T, ?> getPrevious() {
		return previous;
	}

	public FilterFunction<F> getFilterFunction() {
		return filterFunction;
	}

	public Time getWindowTime() {
		return windowTime;
	}

	public Pattern<T, F> where(FilterFunction<F> newFilterFunction) {
		ClosureCleaner.clean(newFilterFunction, true);

		if (this.filterFunction == null) {
			this.filterFunction = newFilterFunction;
		} else {
			this.filterFunction = new AndFilterFunction<F>(this.filterFunction, newFilterFunction);
		}

		return this;
	}

	public Pattern<T, T> next(final String name) {
		return new Pattern<T, T>(name, this);
	}

	public <S extends F> Pattern<T, S> subtype(final Class<S> subtypeClass) {
		if (filterFunction == null) {
			this.filterFunction = new SubtypeFilterFunction<F>(subtypeClass);
		} else {
			this.filterFunction = new AndFilterFunction<F>(this.filterFunction, new SubtypeFilterFunction<F>(subtypeClass));
		}

		@SuppressWarnings("unchecked")
		Pattern<T, S> result = (Pattern<T, S>) this;

		return result;
	}

	public Pattern<T, F> within(Time windowTime) {
		if (windowTime != null) {
			this.windowTime = windowTime;
		}

		return this;
	}

	public FollowedByPattern<T, T> followedBy(final String name) {
		return new FollowedByPattern<T, T>(name, this);
	}

	public static <X> Pattern<X, X> begin(final String name) {
		return new Pattern<X, X>(name, null);
	}

}
