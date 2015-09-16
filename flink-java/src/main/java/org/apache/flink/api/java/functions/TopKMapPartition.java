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
package org.apache.flink.api.java.functions;

import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.flink.util.PriorityQueue;

public class TopKMapPartition<T> extends RichMapPartitionFunction<T, T> {
	private final TypeInformation<T> typeInformation;
	private final int k;
	private final boolean order;

	public TopKMapPartition(TypeInformation<T> typeInformation, int k, boolean order) {
		this.typeInformation = typeInformation;
		this.k = k;
		this.order = order;
	}

	@Override
	public void mapPartition(Iterable<T> values, Collector<T> out) throws Exception {
		// PriorityQueue poll smallest element every time, so we reverse the order here.
		PriorityQueue<T> priorityQueue = getRuntimeContext().getPriorityQueue(this.typeInformation, k, !this.order);

		for (T value : values) {
			priorityQueue.insert(value);
		}

		int count = k;
		T element = priorityQueue.next();
		while (element != null && count > 0) {
			out.collect(element);
			element = priorityQueue.next();
			count--;
		}

		priorityQueue.close();
	}
}
