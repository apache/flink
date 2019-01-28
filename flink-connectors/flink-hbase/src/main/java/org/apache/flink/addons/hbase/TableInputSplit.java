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

package org.apache.flink.addons.hbase;

import org.apache.flink.core.io.LocatableInputSplit;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;

/**
 * This class implements a input splits for HBase. Each table input split corresponds to a key range (low, high). All
 * references to row below refer to the key of the row.
 */
public class TableInputSplit extends LocatableInputSplit {

	private static final long serialVersionUID = 1L;

	/** The name of the table to retrieve data from. */
	private final byte[] tableName;

	/** The start row of the split. */
	private final byte[] startRow;

	/** The end row of the split. */
	private final byte[] endRow;

	// *************************************************************************
	//  The following variables are derived from HRegionInfo class.
	//  They are used in TableSnapshotInputFormat class, for TableInputFormat
	//  those values will be left with default or empty.
	// *************************************************************************

	/** The region id. */
	private long regionId = -1;

	/** The region name. */
	private byte[] regionName = HConstants.EMPTY_BYTE_ARRAY;

	/** The split flag. If the split has daughters, then value is true. */
	private boolean split = false;

	/** The region encoded name. */
	private String encodedName = null;

	/** The replica id. */
	private int replicaId = 0;

	/**
	 * Creates a new table input split.
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
	public TableInputSplit(final int splitNumber, final String[] hostnames, final byte[] tableName, final byte[] startRow,
					final byte[] endRow) {
		super(splitNumber, hostnames);

		this.tableName = tableName;
		this.startRow = startRow;
		this.endRow = endRow;
	}

	/**
	 * Creates a new table input split.
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
	 * @param hRegionInfo
	 * 	      the hregion info
	 */
	public TableInputSplit(final int splitNumber, final String[] hostnames, final byte[] tableName, final byte[] startRow,
					final byte[] endRow, final HRegionInfo hRegionInfo) {
		this(splitNumber, hostnames, tableName, startRow, endRow);
		this.regionId = hRegionInfo.getRegionId();
		this.regionName = hRegionInfo.getRegionName();
		this.split = hRegionInfo.isSplit();
		this.encodedName = hRegionInfo.getEncodedName();
		this.replicaId = hRegionInfo.getReplicaId();
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
	 * Returns the region id.
	 *
	 * @return The region id.
	 */
	public long getRegionId() {
		return regionId;
	}

	/**
	 * Returns the region name.
	 *
	 * @return The region name.
	 */
	public byte[] getRegionName() {
		return regionName;
	}

	/**
	 * Returns if the region has daughters.
	 *
	 * @return The split flag which indicates whether the split has daughters.
	 */
	public boolean isSplit() {
		return split;
	}

	/**
	 * Returns the encoded name.
	 *
	 * @return The encoded name.
	 */
	public String getEncodedName() {
		return encodedName;
	}

	/**
	 * Returns the replica id.
	 *
	 * @return The replica id.
	 */
	public int getReplicaId() {
		return replicaId;
	}

}
