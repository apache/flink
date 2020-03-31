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

package org.apache.flink.table.runtime.arrow.readers;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.runtime.arrow.ArrowReader;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

/**
 * {@link ArrowReader} which read the underlying Arrow format data as {@link Row}.
 */
@Internal
public final class RowArrowReader implements ArrowReader<Row> {

	/**
	 * An array of readers which are responsible for the deserialization of each column of the rows.
	 */
	private final ArrowFieldReader[] fieldReaders;

	/**
	 * Reusable row used to hold the deserialized result.
	 */
	private final Row reuseRow;

	public RowArrowReader(ArrowFieldReader[] fieldReaders) {
		this.fieldReaders = Preconditions.checkNotNull(fieldReaders);
		this.reuseRow = new Row(fieldReaders.length);
	}

	/**
	 * Gets the field readers.
	 */
	public ArrowFieldReader[] getFieldReaders() {
		return fieldReaders;
	}

	@Override
	public Row read(int rowId) {
		for (int i = 0; i < fieldReaders.length; i++) {
			reuseRow.setField(i, fieldReaders[i].read(rowId));
		}
		return reuseRow;
	}
}
