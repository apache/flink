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

package org.apache.flink.table.connector.format;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.types.DataType;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A {@link Format} for a {@link DynamicTableSource} for reading rows.
 *
 * @param <I> runtime interface needed by the table source
 */
@PublicEvolving
public interface DecodingFormat<I> extends Format {

	/**
	 * Creates runtime decoder implementation that is configured to produce data of the given data type.
	 */
	I createRuntimeDecoder(DynamicTableSource.Context context, DataType producedDataType);

	/**
	 * Returns the map of metadata keys and their corresponding data types that can be produced by this
	 * format for reading. By default, this method returns an empty map.
	 *
	 * <p>Metadata columns add additional columns to the table's schema. A decoding format is responsible
	 * to add requested metadata columns at the end of produced rows.
	 *
	 * <p>See {@link SupportsReadingMetadata} for more information.
	 *
	 * <p>Note: This method is only used if the outer {@link DynamicTableSource} implements {@link SupportsReadingMetadata}
	 * and calls this method in {@link SupportsReadingMetadata#listReadableMetadata()}.
	 */
	default Map<String, DataType> listReadableMetadata() {
		return Collections.emptyMap();
	}

	/**
	 * Provides a list of metadata keys that the produced row must contain as appended metadata columns.
	 * By default, this method throws an exception if metadata keys are defined.
	 *
	 * <p>See {@link SupportsReadingMetadata} for more information.
	 *
	 * <p>Note: This method is only used if the outer {@link DynamicTableSource} implements {@link SupportsReadingMetadata}
	 * and calls this method in {@link SupportsReadingMetadata#applyReadableMetadata(List, DataType)}.
	 */
	@SuppressWarnings("unused")
	default void applyReadableMetadata(List<String> metadataKeys) {
		throw new UnsupportedOperationException(
			"A decoding format must override this method to apply metadata keys.");
	}
}
