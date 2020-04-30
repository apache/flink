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

package org.apache.flink.api.common.functions.util;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * A {@link Collector} that puts the collected elements into a given list.
 *
 * @param <T> The type of the collected elements.
 */
@Internal
public class ListCollector<T> implements Collector<T> {

	private final List<T> list;

	public ListCollector(List<T> list) {
		this.list = list;
	}

	@Override
	public void collect(T record) {
		list.add(record);
	}

	@Override
	public void close() {}
}
