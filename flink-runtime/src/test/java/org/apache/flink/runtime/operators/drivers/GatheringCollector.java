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


package org.apache.flink.runtime.operators.drivers;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.util.Collector;

public class GatheringCollector<T> implements Collector<T> {

	private final List<T> list = new ArrayList<T>();
	
	private final TypeSerializer<T> serializer;

	public GatheringCollector(TypeSerializer<T> serializer) {
		this.serializer = serializer;
	}
	
	public List<T> getList() {
		return list;
	}

	@Override
	public void collect(T record) {
		T copy = this.serializer.createInstance();
		this.list.add(this.serializer.copy(record, copy));
	}

	@Override
	public void close() {}
}
