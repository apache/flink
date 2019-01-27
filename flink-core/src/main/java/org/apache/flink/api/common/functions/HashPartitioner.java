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

import org.apache.flink.util.MathUtils;

import java.util.Objects;

/**
 * A {@link Partitioner} which partition objects according to their hash code.
 *
 * @param <T> Type of the objects to be partitioned.
 */
public class HashPartitioner<T> implements Partitioner<T> {

	private static final long serialVersionUID = 1L;

	public static final HashPartitioner<Object> INSTANCE = new HashPartitioner<>();

	@Override
	public int partition(T key, int numPartitions) {
		return MathUtils.murmurHash(Objects.hashCode(key)) % numPartitions;
	}

	@Override
	public boolean equals(Object obj) {
		return (obj == this) || (obj != null && obj.getClass() == getClass());
	}

	@Override
	public int hashCode() {
		return "HashPartitioner".hashCode();
	}

	@Override
	public String toString() {
		return "HashPartitioner";
	}
}
