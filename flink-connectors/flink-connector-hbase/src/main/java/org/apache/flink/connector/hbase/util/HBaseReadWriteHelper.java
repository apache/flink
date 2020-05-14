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

package org.apache.flink.connector.hbase.util;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.types.Row;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;

import java.nio.charset.Charset;

/**
 * A read and write helper for HBase. The helper can used to create a {@link Scan} and {@link Get}
 * for scanning or lookuping a HBase table, and create a {@link Put} and {@link Delete} for writing
 * to HBase table, and supports converting the HBase {@link Result} to Flink {@link Row}.
 */
@Internal
public class HBaseReadWriteHelper {

	// family keys
	private final byte[][] families;
	// qualifier keys
	private final byte[][][] qualifiers;
	// qualifier types
	private final int[][] qualifierTypes;

	// row key index in output row
	private final int rowKeyIndex;
	// type of row key
	private final int rowKeyType;

	private final int fieldLength;
	private final Charset charset;

	// row which is returned
	private Row resultRow;
	// nested family rows
	private Row[] familyRows;

	public HBaseReadWriteHelper(HBaseTableSchema hbaseTableSchema) {
		this.families = hbaseTableSchema.getFamilyKeys();
		this.qualifiers = new byte[this.families.length][][];
		this.qualifierTypes = new int[this.families.length][];
		this.familyRows = new Row[this.families.length];
		String[] familyNames = hbaseTableSchema.getFamilyNames();
		for (int f = 0; f < families.length; f++) {
			this.qualifiers[f] = hbaseTableSchema.getQualifierKeys(familyNames[f]);
			TypeInformation[] typeInfos = hbaseTableSchema.getQualifierTypes(familyNames[f]);
			this.qualifierTypes[f] = new int[typeInfos.length];
			for (int i = 0; i < typeInfos.length; i++) {
				qualifierTypes[f][i] = HBaseTypeUtils.getTypeIndex(typeInfos[i]);
			}
			this.familyRows[f] = new Row(typeInfos.length);
		}
		this.charset = Charset.forName(hbaseTableSchema.getStringCharset());
		// row key
		this.rowKeyIndex = hbaseTableSchema.getRowKeyIndex();
		this.rowKeyType = hbaseTableSchema.getRowKeyTypeInfo()
			.map(HBaseTypeUtils::getTypeIndex)
			.orElse(-1);

		// field length need take row key into account if it exists.
		this.fieldLength = rowKeyIndex == -1 ? families.length : families.length + 1;

		// prepare output rows
		this.resultRow = new Row(fieldLength);
	}

	/**
	 * Returns an instance of Get that retrieves the matches records from the HBase table.
	 *
	 * @return The appropriate instance of Get for this use case.
	 */
	public Get createGet(Object rowKey) {
		byte[] rowkey = HBaseTypeUtils.serializeFromObject(
			rowKey,
			rowKeyType,
			charset);
		Get get = new Get(rowkey);
		for (int f = 0; f < families.length; f++) {
			byte[] family = families[f];
			for (byte[] qualifier : qualifiers[f]) {
				get.addColumn(family, qualifier);
			}
		}
		return get;
	}

	/**
	 * Returns an instance of Scan that retrieves the required subset of records from the HBase table.
	 *
	 * @return The appropriate instance of Scan for this use case.
	 */
	public Scan createScan() {
		Scan scan = new Scan();
		for (int f = 0; f < families.length; f++) {
			byte[] family = families[f];
			for (int q = 0; q < qualifiers[f].length; q++) {
				byte[] quantifier = qualifiers[f][q];
				scan.addColumn(family, quantifier);
			}
		}
		return scan;
	}

	/**
	 * Parses HBase {@link Result} into {@link Row}.
	 */
	public Row parseToRow(Result result) {
		if (rowKeyIndex == -1) {
			return parseToRow(result, null);
		} else {
			Object rowkey = HBaseTypeUtils.deserializeToObject(result.getRow(), rowKeyType, charset);
			return parseToRow(result, rowkey);
		}
	}

	/**
	 * Parses HBase {@link Result} into {@link Row}.
	 */
	public Row parseToRow(Result result, Object rowKey) {
		for (int i = 0; i < fieldLength; i++) {
			if (rowKeyIndex == i) {
				resultRow.setField(rowKeyIndex, rowKey);
			} else {
				int f = (rowKeyIndex != -1 && i > rowKeyIndex) ? i - 1 : i;
				// get family key
				byte[] familyKey = families[f];
				Row familyRow = familyRows[f];
				for (int q = 0; q < this.qualifiers[f].length; q++) {
					// get quantifier key
					byte[] qualifier = qualifiers[f][q];
					// get quantifier type idx
					int typeIdx = qualifierTypes[f][q];
					// read value
					byte[] value = result.getValue(familyKey, qualifier);
					if (value != null) {
						familyRow.setField(q, HBaseTypeUtils.deserializeToObject(value, typeIdx, charset));
					} else {
						familyRow.setField(q, null);
					}
				}
				resultRow.setField(i, familyRow);
			}
		}
		return resultRow;
	}

	/**
	 * Returns an instance of Put that writes record to HBase table.
	 *
	 * @return The appropriate instance of Put for this use case.
	 */
	public Put createPutMutation(Row row) {
		assert rowKeyIndex != -1;
		byte[] rowkey = HBaseTypeUtils.serializeFromObject(row.getField(rowKeyIndex), rowKeyType, charset);
		// upsert
		Put put = new Put(rowkey);
		for (int i = 0; i < fieldLength; i++) {
			if (i != rowKeyIndex) {
				int f = i > rowKeyIndex ? i - 1 : i;
				// get family key
				byte[] familyKey = families[f];
				Row familyRow = (Row) row.getField(i);
				for (int q = 0; q < this.qualifiers[f].length; q++) {
					// get quantifier key
					byte[] qualifier = qualifiers[f][q];
					// get quantifier type idx
					int typeIdx = qualifierTypes[f][q];
					// read value
					byte[] value = HBaseTypeUtils.serializeFromObject(familyRow.getField(q), typeIdx, charset);
					put.addColumn(familyKey, qualifier, value);
				}
			}
		}
		return put;
	}

	/**
	 * Returns an instance of Delete that remove record from HBase table.
	 *
	 * @return The appropriate instance of Delete for this use case.
	 */
	public Delete createDeleteMutation(Row row) {
		assert rowKeyIndex != -1;
		byte[] rowkey = HBaseTypeUtils.serializeFromObject(row.getField(rowKeyIndex), rowKeyType, charset);
		// delete
		Delete delete = new Delete(rowkey);
		for (int i = 0; i < fieldLength; i++) {
			if (i != rowKeyIndex) {
				int f = i > rowKeyIndex ? i - 1 : i;
				// get family key
				byte[] familyKey = families[f];
				for (int q = 0; q < this.qualifiers[f].length; q++) {
					// get quantifier key
					byte[] qualifier = qualifiers[f][q];
					delete.addColumn(familyKey, qualifier);
				}
			}
		}
		return delete;
	}
}
