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

package org.apache.flink.api.common.functions.util.test;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.List;

/**
 * A simple {@link ListState} for testing.
 */
public class SimpleListState<T> implements ListState<T> {

	private final List<T> list = new ArrayList<>();

	@Override
	public void clear() {
		list.clear();
	}

	@Override
	public Iterable<T> get() throws Exception {
		return list;
	}

	@Override
	public void add(T value) throws Exception {
		Preconditions.checkNotNull(value, "You cannot add null to a ListState.");
		list.add(value);
	}

	public List<T> getList() {
		return list;
	}

	@Override
	public void update(List<T> values) throws Exception {
		clear();

		addAll(values);
	}

	@Override
	public void addAll(List<T> values) throws Exception {
		if (values != null) {
			values.forEach(v -> Preconditions.checkNotNull(v, "You cannot add null to a ListState."));
			list.addAll(values);
		}
	}
}
