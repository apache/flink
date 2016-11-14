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

package org.apache.flink.api.common.accumulators;

import org.apache.flink.annotation.Public;

import java.util.ArrayList;

/**
 * This accumulator stores a collection of objects.
 *
 * @param <T> The type of the accumulated objects
 */
@Public
public class ListAccumulator<T> implements Accumulator<T, ArrayList<T>> {

	private static final long serialVersionUID = 1L;

	private ArrayList<T> localValue = new ArrayList<T>();
	
	@Override
	public void add(T value) {
		localValue.add(value);
	}

	@Override
	public ArrayList<T> getLocalValue() {
		return localValue;
	}

	@Override
	public void resetLocal() {
		localValue.clear();
	}

	@Override
	public void merge(Accumulator<T, ArrayList<T>> other) {
		localValue.addAll(other.getLocalValue());
	}

	@Override
	public Accumulator<T, ArrayList<T>> clone() {
		ListAccumulator<T> newInstance = new ListAccumulator<T>();
		newInstance.localValue = new ArrayList<T>(localValue);
		return newInstance;
	}

	@Override
	public String toString() {
		return "List Accumulator " + localValue;
	}
}
