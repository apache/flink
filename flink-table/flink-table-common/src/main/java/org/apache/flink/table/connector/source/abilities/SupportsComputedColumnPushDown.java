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

package org.apache.flink.table.connector.source.abilities;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.RuntimeConverter;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import java.io.Serializable;

/**
 * Enables to push down computed columns into a {@link ScanTableSource}.
 *
 * <p>Computed columns add additional columns to the table's schema. They are defined by logical expressions
 * that reference other physically existing columns.
 *
 * <p>An example in SQL looks like:
 * <pre>{@code
 *   CREATE TABLE t (str STRING, ts AS PARSE_TIMESTAMP(str), i INT)   // `ts` is a computed column
 * }</pre>
 *
 * <p>By default, if this interface is not implemented, computed columns are added to the physically produced
 * row in a subsequent operation after the source.
 *
 * <p>However, it might be beneficial to perform the computation as early as possible in order to be
 * close to the actual data generation. Especially in cases where computed columns are used for generating
 * watermarks, a source must push down the computation as deep as possible such that the computation
 * can happen within a source's data partition.
 *
 * <p>This interface provides a {@link ComputedColumnConverter} that needs to be applied to every row
 * during runtime.
 *
 * <p>Note: The final output data type emitted by a source changes from the physically produced data
 * type to the full data type of the table's schema. For the example above, this means:
 *
 * <pre>{@code
 *    ROW<str STRING, i INT>                    // before conversion
 *    ROW<str STRING, ts TIMESTAMP(3), i INT>   // after conversion
 * }</pre>
 *
 * <p>Note: If a source implements {@link SupportsProjectionPushDown}, the projection must be applied to
 * the physical data in the first step. The {@link SupportsComputedColumnPushDown} (already aware of the
 * projection) will then use the projected physical data and insert computed columns into the result. In
 * the example below, the projections {@code [i, d]} are derived from the DDL ({@code c} requires {@code i})
 * and query ({@code d} and {@code c} are required). The pushed converter will rely on this order and
 * will process {@code [i, d]} to produce {@code [d, c]}.
 * <pre>{@code
 *   CREATE TABLE t (i INT, s STRING, c AS i + 2, d DOUBLE);
 *   SELECT d, c FROM t;
 * }</pre>
 */
@PublicEvolving
public interface SupportsComputedColumnPushDown {

	/**
	 * Provides a converter that converts the produced {@link RowData} containing the physical
	 * fields of the external system into a new {@link RowData} with push-downed computed columns.
	 *
	 * <p>Note: Use the passed data type instead of {@link TableSchema#toPhysicalRowDataType()} for
	 * describing the final output data type when creating {@link TypeInformation}. If the source implements
	 * {@link SupportsProjectionPushDown}, the projection is already considered in both the converter
	 * and the given output data type.
	 */
	void applyComputedColumn(ComputedColumnConverter converter, DataType outputDataType);

	/**
	 * Generates and adds computed columns to {@link RowData} if necessary.
	 *
	 * <p>Instances of this interface are {@link Serializable} and can be directly passed into the runtime
	 * implementation class.
	 */
	interface ComputedColumnConverter extends RuntimeConverter {

		/**
		 * Generates and adds computed columns to {@link RowData} if necessary.
		 *
		 * @param producedRow physical row read from the external system
		 * @return row enriched with computed columns
		 */
		RowData convert(RowData producedRow);
	}
}
