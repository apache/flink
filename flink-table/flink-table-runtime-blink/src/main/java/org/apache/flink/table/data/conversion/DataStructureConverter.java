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

package org.apache.flink.table.data.conversion;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import java.io.Serializable;

/**
 * Converter between internal and external data structure.
 *
 * <p>Converters are serializable and can be passed to runtime operators.
 *
 * @param <I> internal data structure (see {@link RowData})
 * @param <E> external data structure (see {@link DataType#getConversionClass()})
 */
@Internal
public interface DataStructureConverter<I, E> extends Serializable {

	default void open(ClassLoader classLoader) {
		assert classLoader != null;
		// nothing to do
	}

	/**
	 * Converts to internal data structure.
	 *
	 * <p>Note: Parameter must not be null. Output must not be null.
	 */
	I toInternal(E external);

	/**
	 * Converts to internal data structure or {@code null}.
	 *
	 * <p>The nullability could be derived from the data type. However, this method reduces null checks.
	 */
	default I toInternalOrNull(E external) {
		if (external == null) {
			return null;
		}
		return toInternal(external);
	}

	/**
	 * Converts to external data structure.
	 *
	 * <p>Note: Parameter must not be null. Output must not be null.
	 */
	E toExternal(I internal);

	/**
	 * Converts to external data structure or {@code null}.
	 *
	 * <p>The nullability could be derived from the data type. However, this method reduces null checks.
	 */
	default E toExternalOrNull(I internal) {
		if (internal == null) {
			return null;
		}
		return toExternal(internal);
	}
}
