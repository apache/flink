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

import org.apache.flink.types.RowKind;

/**
 * An interface for row used internally in Flink Table/SQL.
 *
 * <p>There are different implementations depending on the scenario:
 * After serialization, it becomes the BinaryRow format.
 * Convenient updates use the GenericRow format.
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
	 * Returns the kind of change that this row describes in a changelog.
	 *
	 * @see RowKind
	 */
	RowKind getRowKind();

	/**
	 * Sets the kind of change that this row describes in a changelog.
	 *
	 * @see RowKind
	 */
	void setRowKind(RowKind kind);
}
