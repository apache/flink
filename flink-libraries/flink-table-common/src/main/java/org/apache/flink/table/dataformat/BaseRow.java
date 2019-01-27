/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.	See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.	You may obtain a copy of the License at
 *
 *		http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.dataformat;

/**
 * An interface for row used internally in Flink Table/SQL.
 *
 * <p>There are different implementations depending on the scenario:
 * After serialization, it becomes the {@link BinaryRow} format.
 * Convenient updates use the {@link GenericRow} format.
 * Splicing two Rows using the {@link JoinedRow} format.
 * Column storage uses {@link ColumnarRow} format.
 * Avoiding unboxing and boxing using the {@link BoxedWrapperRow} format.
 *
 * <p>{@code BaseRow}s are influenced by Apache Spark InternalRows.
 */
public interface BaseRow extends TypeGetterSetters {

	/**
	 * Get the number of fields in the BaseRow.
	 *
	 * @return The number of fields in the BaseRow.
	 */
	int getArity();

	/**
	 * The header represents the type of this Row. Now just used in streaming.
	 * Now there are two message: ACCUMULATE_MSG and RETRACT_MSG.
	 */
	byte getHeader();

	/**
	 * Set the byte header.
	 */
	void setHeader(byte header);

	/**
	 * Because the specific row implementation such as BinaryRow uses the binary format. We must
	 * first determine if it is null, and then make a specific get.
	 *
	 * @return true if this field is null.
	 */
	boolean isNullAt(int ordinal);

	/**
	 * Set null to this field.
	 */
	void setNullAt(int ordinal);

	/**
	 * Equals without header compare. Do not compare their types, only compare internal values.
	 */
	boolean equalsWithoutHeader(BaseRow other);
}
