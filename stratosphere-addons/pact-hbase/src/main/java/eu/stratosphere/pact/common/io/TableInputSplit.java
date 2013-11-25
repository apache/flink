/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.pact.common.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.nephele.template.LocatableInputSplit;

/**
 * This class implements a input splits for HBase. Each table input split corresponds to a key range (low, high). All
 * references to row below refer to the key of the row.
 * 
 * @author warneke
 */
public class TableInputSplit extends LocatableInputSplit {

	/**
	 * The name of the table to retrieve data from
	 */
	private byte[] tableName;

	/**
	 * The start row of the split.
	 */
	private byte[] startRow;

	/**
	 * The end row of the split.
	 */
	private byte[] endRow;

	/**
	 * Creates a new table input split
	 * 
	 * @param splitNumber
	 *        the number of the input split
	 * @param hostnames
	 *        the names of the hosts storing the data the input split refers to
	 * @param tableName
	 *        the name of the table to retrieve data from
	 * @param startRow
	 *        the start row of the split
	 * @param endRow
	 *        the end row of the split
	 */
	TableInputSplit(final int splitNumber, final String[] hostnames, final byte[] tableName, final byte[] startRow,
			final byte[] endRow) {
		super(splitNumber, hostnames);

		this.tableName = tableName;
		this.startRow = startRow;
		this.endRow = endRow;
	}

	/**
	 * Default constructor for serialization/deserialization.
	 */
	public TableInputSplit() {
		super();

		this.tableName = null;
		this.startRow = null;
		this.endRow = null;
	}

	/**
	 * Returns the table name.
	 * 
	 * @return The table name.
	 */
	public byte[] getTableName() {
		return this.tableName;
	}

	/**
	 * Returns the start row.
	 * 
	 * @return The start row.
	 */
	public byte[] getStartRow() {
		return this.startRow;
	}

	/**
	 * Returns the end row.
	 * 
	 * @return The end row.
	 */
	public byte[] getEndRow() {
		return this.endRow;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final DataOutput out) throws IOException {

		super.write(out);

		// Write the table name
		if (this.tableName == null) {
			out.writeInt(-1);
		} else {
			out.writeInt(this.tableName.length);
			out.write(this.tableName);
		}

		// Write the start row
		if (this.startRow == null) {
			out.writeInt(-1);
		} else {
			out.writeInt(this.startRow.length);
			out.write(this.startRow);
		}

		// Write the end row
		if (this.endRow == null) {
			out.writeInt(-1);
		} else {
			out.writeInt(this.endRow.length);
			out.write(this.endRow);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void read(final DataInput in) throws IOException {

		super.read(in);

		// Read the table name
		int len = in.readInt();
		if (len >= 0) {
			this.tableName = new byte[len];
			in.readFully(this.tableName);
		}

		// Read the start row
		len = in.readInt();
		if (len >= 0) {
			this.startRow = new byte[len];
			in.readFully(this.startRow);
		}

		// Read the end row
		len = in.readInt();
		if (len >= 0) {
			this.endRow = new byte[len];
			in.readFully(this.endRow);
		}
	}
}
