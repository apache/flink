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

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.functions.Comparator;
import org.apache.flink.api.common.functions.Merger;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Objects;

/**
 * The descriptor for the columns in {@link InternalState}s.
 *
 * @param <T> Type of the values in the column.
 */
public class InternalColumnDescriptor<T> implements Serializable {

	private static final long serialVersionUID = 1L;

	/**
	 * The name of the column.
	 */
	private final String name;

	/**
	 * The serializer for the values in the column.
	 */
	private final TypeSerializer<T> serializer;

	/**
	 * The comparator for the values in the column. The values under the same
	 * prefix key will be sorted iff the comparator is not null.
	 */
	@Nullable
	private final Comparator<T> comparator;

	/**
	 * The merger for the values in the column. This is null in the key columns
	 * or those value columns in local states.
	 */
	@Nullable
	private final Merger<T> merger;

	/**
	 * Constructor for the descriptor of unordered key columns or the value
	 * columns in global states.
	 *
	 * @param name The name of the column.
	 * @param serializer The serializer for the objects in the column.
	 */
	public InternalColumnDescriptor(
		String name,
		TypeSerializer<T> serializer
	) {
		Preconditions.checkNotNull(name);
		Preconditions.checkNotNull(serializer);

		this.name = name;
		this.serializer = serializer;
		this.comparator = null;
		this.merger = null;
	}

	/**
	 * Constructor for the descriptor of ordered key columns.
	 *
	 * @param name The name of the column.
	 * @param serializer The serializer for the objects in the column.
	 * @param comparator The comparator for the objects in the column.
	 */
	public InternalColumnDescriptor(
		String name,
		TypeSerializer<T> serializer,
		Comparator<T> comparator
	) {
		Preconditions.checkNotNull(name);
		Preconditions.checkNotNull(serializer);
		Preconditions.checkNotNull(comparator);

		this.name = name;
		this.serializer = serializer;
		this.comparator = comparator;
		this.merger = null;
	}

	/**
	 * Constructor for the descriptor of the value columns in local states.
	 *
	 * @param name The name of the column.
	 * @param serializer The serializer for the objects in the column.
	 * @param merger The merger for the objects in the column.
	 */
	public InternalColumnDescriptor(
		String name,
		TypeSerializer<T> serializer,
		Merger<T> merger
	) {
		Preconditions.checkNotNull(name);
		Preconditions.checkNotNull(serializer);
		Preconditions.checkNotNull(merger);

		this.name = name;
		this.serializer = serializer;
		this.merger = merger;
		this.comparator = null;
	}

	/**
	 * Returns the name of the column.
	 *
	 * @return The name of the column.
	 */
	public String getName() {
		return name;
	}

	/**
	 * Returns the serializer for the objects in the column.
	 *
	 * @return The serializer for the objects in the column.
	 */
	public TypeSerializer<T> getSerializer() {
		return serializer;
	}

	/**
	 * Returns the comparator for the objects in the column.
	 *
	 * @return The comparator for the objects in the column.
	 */
	public Comparator<T> getComparator() {
		return comparator;
	}

	/**
	 * Returns the merger for the objects in the column.
	 *
	 * @return The merger for the objects in the column.
	 */
	public Merger<T> getMerger() {
		return merger;
	}

	/**
	 * Returns true if the column is ordered under the same prefix key. Values
	 * are always unordered under the same key.
	 *
	 * @return True if the column is ordered under the same prefix.
	 */
	public boolean isOrdered() {
		return (comparator != null);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}

		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		InternalColumnDescriptor<?> that = (InternalColumnDescriptor<?>) o;

		return Objects.equals(name, that.name) &&
			Objects.equals(serializer, that.serializer) &&
			Objects.equals(comparator, that.comparator) &&
			Objects.equals(merger, that.merger);
	}

	@Override
	public int hashCode() {
		int result = Objects.hashCode(name);
		result = 31 * result + Objects.hashCode(serializer);
		result = 31 * result + Objects.hashCode(comparator);
		result = 31 * result + Objects.hashCode(merger);
		return result;
	}

	@Override
	public String toString() {
		return "InternalColumnDescriptor{" +
			"name=" + name +
			", serializer=" + serializer +
			", comparator=" + comparator +
			", merger=" + merger +
			"}";
	}
}

