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

package org.apache.flink.runtime.operators.sort;

import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerFactory;
import org.apache.flink.core.memory.MemorySegment;

import javax.annotation.Nonnull;

import java.util.List;

/**
 * Default factory for {@link InMemorySorter}.
 */
public class DefaultInMemorySorterFactory<T> implements InMemorySorterFactory<T> {

	@Nonnull
	private final TypeSerializerFactory<T> typeSerializerFactory;

	@Nonnull
	private final TypeComparator<T> typeComparator;

	private final boolean useFixedLengthRecordSorter;

	DefaultInMemorySorterFactory(
			@Nonnull TypeSerializerFactory<T> typeSerializerFactory,
			@Nonnull TypeComparator<T> typeComparator,
			int thresholdForInPlaceSorting) {
		this.typeSerializerFactory = typeSerializerFactory;
		this.typeComparator = typeComparator;

		TypeSerializer<T> typeSerializer = typeSerializerFactory.getSerializer();

		this.useFixedLengthRecordSorter = typeComparator.supportsSerializationWithKeyNormalization() &&
			typeSerializer.getLength() > 0 && typeSerializer.getLength() <= thresholdForInPlaceSorting;
	}

	@Override
	public InMemorySorter<T> create(List<MemorySegment> sortSegments) {
		final TypeSerializer<T> typeSerializer = typeSerializerFactory.getSerializer();
		final TypeComparator<T> duplicateTypeComparator = typeComparator.duplicate();

		if (useFixedLengthRecordSorter) {
			return new FixedLengthRecordSorter<>(typeSerializer, duplicateTypeComparator, sortSegments);
		} else {
			return new NormalizedKeySorter<>(typeSerializer, duplicateTypeComparator, sortSegments);
		}
	}
}
