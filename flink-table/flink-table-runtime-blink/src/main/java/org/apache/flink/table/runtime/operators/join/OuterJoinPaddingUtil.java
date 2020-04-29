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

package org.apache.flink.table.runtime.operators.join;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.JoinedRowData;
import org.apache.flink.table.data.RowData;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;

/**
 * An utility to generate reusable padding results for outer joins.
 */
public class OuterJoinPaddingUtil implements Serializable {

	private static final long serialVersionUID = -2295909099427806938L;

	private final int leftArity;
	private final int rightArity;
	private transient JoinedRowData joinedRow = new JoinedRowData();
	private transient GenericRowData leftNullPaddingRow;
	private transient GenericRowData rightNullPaddingRow;

	public OuterJoinPaddingUtil(int leftArity, int rightArity) {
		this.leftArity = leftArity;
		this.rightArity = rightArity;
		initLeftNullPaddingRow();
		initRightNullPaddingRow();
	}

	private void initLeftNullPaddingRow() {
		//Initialize the two reusable padding results
		leftNullPaddingRow = new GenericRowData(leftArity);
		for (int idx = 0; idx < leftArity; idx++) {
			leftNullPaddingRow.setField(idx, null);
		}
	}

	private void initRightNullPaddingRow() {
		rightNullPaddingRow = new GenericRowData(rightArity);
		for (int idx = 0; idx < rightArity; idx++) {
			rightNullPaddingRow.setField(idx, null);
		}
	}

	/**
	 * Returns a padding result with the given right row.
	 *
	 * @param rightRow the right row to pad
	 * @return the reusable null padding result
	 */
	public final RowData padRight(RowData rightRow) {
		return joinedRow.replace(leftNullPaddingRow, rightRow);
	}

	/**
	 * Returns a padding result with the given left row.
	 *
	 * @param leftRow the left row to pad
	 * @return the reusable null padding result
	 */
	public final RowData padLeft(RowData leftRow) {
		return joinedRow.replace(leftRow, rightNullPaddingRow);
	}

	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
		in.defaultReadObject();

		joinedRow = new JoinedRowData();
		initLeftNullPaddingRow();
		initRightNullPaddingRow();
	}
}
