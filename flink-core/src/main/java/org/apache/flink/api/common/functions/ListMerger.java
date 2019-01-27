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

import org.apache.flink.util.Preconditions;

import java.util.List;

/**
 * A {@link Merger} for lists which merges two lists by appending the elements
 * in the second list to the first one.
 *
 * @param <E> Type of the elements in the lists to be merged.
 */
public final class ListMerger<E> implements Merger<List<E>>{

	private static final long serialVersionUID = 1L;

	@Override
	public List<E> merge(List<E> value1, List<E> value2) {
		Preconditions.checkNotNull(value1);
		Preconditions.checkNotNull(value2);

		value1.addAll(value2);

		return value1;
	}

	@Override
	public boolean equals(Object obj) {
		return (obj == this) || (obj != null && obj.getClass() == getClass());
	}

	@Override
	public int hashCode() {
		return "ListMerger".hashCode();
	}

	@Override
	public String toString() {
		return "ListMerger";
	}
}
