/**
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

import java.util.List;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.util.Collector;

public class CopyingListCollector<T> implements Collector<T> {

	private final List<T> list;
	private final TypeSerializer<T> serializer;

	public CopyingListCollector(List<T> list, TypeSerializer<T> serializer) {
		this.list = list;
		this.serializer = serializer;
	}

	@Override
	public void collect(T record) {
		list.add(serializer.copy(record));
	}

	@Override
	public void close() {}
}
