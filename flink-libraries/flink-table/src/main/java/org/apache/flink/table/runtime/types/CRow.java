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

package org.apache.flink.table.runtime.types;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.types.Row;

/**
 * Wrapper for a [[Row]] to add retraction information.
 *
 * <p>If [[change]] is true, the [[CRow]] is an accumulate message, if it is false it is a
 * retraction message.
 *
 * <p>row The wrapped [[Row]].
 * change true for an accumulate message, false for a retraction message.
 */
@PublicEvolving
public class CRow {

	public Row row;

	public Boolean change;

	public CRow() {
		this(null, true);
	}

	public CRow(Row row, boolean change) {
		this.row = row;
		this.change = change;
	}

	public static CRow of() {
		return new CRow();
	}

	public static CRow of(Row row, Boolean change) {
		return new CRow(row, change);
	}

	public static CRow of(Boolean change, Object... values) {
		return new CRow(Row.of(values), change);
	}

	public static CRow of(Object... values) {
		return new CRow(Row.of(values), true);
	}

	@Override
	public boolean equals(Object other) {
		CRow otherCRow = (CRow) other;
		return row.equals(otherCRow.row) && change.equals(otherCRow.change);
	}

	@Override
	public String toString() {
		return change ? "+" + row : "-" + row;
	}

	public Row getRow() {
		return row;
	}

	public Boolean getChange() {
		return change;
	}
}
