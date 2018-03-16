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

import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

/**
 * {@link InputFormat} subclass that wraps the access for HTables. Returns the result as {@link Row}
 */
public class HBaseRowInputFormat extends AbstractTableInputFormat<Row> implements ResultTypeQueryable<Row> {

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(HBaseRowInputFormat.class);
	private String tableName;
	private transient org.apache.hadoop.conf.Configuration conf;
	private HBaseTableSchema schema;
	private transient Charset stringCharset;

	// family keys
	private byte[][] families;
	// qualifier keys
	private byte[][][] qualifiers;
	// qualifier types
	private int[][] types;

	// row which is returned
	private Row resultRow;
	// nested family rows
	private Row[] familyRows;

	public HBaseRowInputFormat(org.apache.hadoop.conf.Configuration conf, String tableName, HBaseTableSchema schema) {
		this.tableName = tableName;
		this.conf = conf;
		this.schema = schema;

		// set families, qualifiers, and types
		String[] familyNames = schema.getFamilyNames();
		this.families = schema.getFamilyKeys();
		this.qualifiers = new byte[this.families.length][][];
		this.types = new int[this.families.length][];
		for (int f = 0; f < families.length; f++) {
			this.qualifiers[f] = schema.getQualifierKeys(familyNames[f]);
			TypeInformation[] typeInfos = schema.getQualifierTypes(familyNames[f]);
			this.types[f] = new int[typeInfos.length];
			for (int i = 0; i < typeInfos.length; i++) {
				int typeIdx = getTypeIndex(typeInfos[i].getTypeClass());
				if (typeIdx >= 0) {
					types[f][i] = typeIdx;
				} else {
					throw new IllegalArgumentException("Unsupported type: " + typeInfos[i]);
				}
			}
		}
	}

	@Override
	public void configure(Configuration parameters) {
		LOG.info("Initializing HBase configuration.");
		connectToTable();
		if (table != null) {
			scan = getScanner();
		}

		// prepare output rows
		this.resultRow = new Row(families.length);
		this.familyRows = new Row[families.length];
		for (int f = 0; f < families.length; f++) {
			this.familyRows[f] = new Row(qualifiers[f].length);
			this.resultRow.setField(f, this.familyRows[f]);
		}

		this.stringCharset = Charset.forName(schema.getStringCharset());
	}

	@Override
	protected Scan getScanner() {
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

	@Override
	public String getTableName() {
		return tableName;
	}

	@Override
	protected Row mapResultToOutType(Result res) {
		for (int f = 0; f < this.families.length; f++) {
			// get family key
			byte[] familyKey = families[f];
			Row familyRow = familyRows[f];
			for (int q = 0; q < this.qualifiers[f].length; q++) {
				// get quantifier key
				byte[] qualifier = qualifiers[f][q];
				// get quantifier type idx
				int typeIdx = types[f][q];
				// read value
				byte[] value = res.getValue(familyKey, qualifier);
				if (value != null) {
					familyRow.setField(q, deserialize(value, typeIdx));
				} else {
					familyRow.setField(q, null);
				}
			}
			resultRow.setField(f, familyRow);
		}
		return resultRow;
	}

	private void connectToTable() {

		if (this.conf == null) {
			this.conf = HBaseConfiguration.create();
		}

		try {
			Connection conn = ConnectionFactory.createConnection(conf);
			super.table = (HTable) conn.getTable(TableName.valueOf(tableName));
		} catch (TableNotFoundException tnfe) {
			LOG.error("The table " + tableName + " not found ", tnfe);
			throw new RuntimeException("HBase table '" + tableName + "' not found.", tnfe);
		} catch (IOException ioe) {
			LOG.error("Exception while creating connection to HBase.", ioe);
			throw new RuntimeException("Cannot create connection to HBase.", ioe);
		}
	}

	@Override
	public TypeInformation<Row> getProducedType() {
		// split the fieldNames
		String[] famNames = schema.getFamilyNames();
		TypeInformation<?>[] typeInfos = new TypeInformation[famNames.length];
		int i = 0;
		for (String family : famNames) {
			typeInfos[i] = new RowTypeInfo(schema.getQualifierTypes(family), schema.getQualifierNames(family));
			i++;
		}
		return new RowTypeInfo(typeInfos, famNames);
	}

	private Object deserialize(byte[] value, int typeIdx) {
		switch (typeIdx) {
			case 0: // byte[]
				return value;
			case 1:
				return new String(value, stringCharset);
			case 2: // byte
				return value[0];
			case 3:
				return Bytes.toShort(value);
			case 4:
				return Bytes.toInt(value);
			case 5:
				return Bytes.toLong(value);
			case 6:
				return Bytes.toFloat(value);
			case 7:
				return Bytes.toDouble(value);
			case 8:
				return Bytes.toBoolean(value);
			case 9: // sql.Timestamp encoded as long
				return new Timestamp(Bytes.toLong(value));
			case 10: // sql.Date encoded as long
				return new Date(Bytes.toLong(value));
			case 11: // sql.Time encoded as long
				return new Time(Bytes.toLong(value));
			case 12:
				return Bytes.toBigDecimal(value);
			case 13:
				return new BigInteger(value);

			default:
				throw new IllegalArgumentException("Unknown type index " + typeIdx);
		}
	}

	private static int getTypeIndex(Class<?> clazz) {
		if (byte[].class.equals(clazz)) {
			return 0;
		} else if (String.class.equals(clazz)) {
			return 1;
		} else if (Byte.class.equals(clazz)) {
			return 2;
		} else if (Short.class.equals(clazz)) {
			return 3;
		} else if (Integer.class.equals(clazz)) {
			return 4;
		} else if (Long.class.equals(clazz)) {
			return 5;
		} else if (Float.class.equals(clazz)) {
			return 6;
		} else if (Double.class.equals(clazz)) {
			return 7;
		} else if (Boolean.class.equals(clazz)) {
			return 8;
		} else if (Timestamp.class.equals(clazz)) {
			return 9;
		} else if (Date.class.equals(clazz)) {
			return 10;
		} else if (Time.class.equals(clazz)) {
			return 11;
		} else if (BigDecimal.class.equals(clazz)) {
			return 12;
		} else if (BigInteger.class.equals(clazz)) {
			return 13;
		} else {
			return -1;
		}
	}

	static boolean isSupportedType(Class<?> clazz) {
		return getTypeIndex(clazz) != -1;
	}

}
