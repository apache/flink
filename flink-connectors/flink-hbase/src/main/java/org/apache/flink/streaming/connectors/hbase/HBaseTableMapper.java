/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.hbase;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.PojoField;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import org.apache.hadoop.hbase.util.Bytes;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * This class is used to record the mapping of a key to column family and qualifier, as well as the table name and
 * rowkey.
 */
public class HBaseTableMapper implements Serializable {

	private final Map<String, Tuple3<String, String, TypeInformation<?>>> keyMap = new TreeMap<>();
	private String rowKey;
	private TypeInformation<?> rowKeyType;
	private String charset = "UTF-8";

	public HBaseTableMapper addMapping(String key, String columnFamily, String qualifier, Class clazz) {
		keyMap.put(key, Tuple3.of(columnFamily, qualifier, TypeExtractor.getForClass(clazz)));
		return this;
	}

	public HBaseTableMapper addMapping(int key, String columnFamily, String qualifier, Class clazz) {
		keyMap.put(String.valueOf(key), Tuple3.of(columnFamily, qualifier, TypeExtractor.getForClass(clazz)));
		return this;
	}

	public HBaseTableMapper setMapping(RowTypeInfo rowTypeInfo, String columnFamily) {
		String[] fieldNames = rowTypeInfo.getFieldNames();
		for (int i = 0; i < rowTypeInfo.getArity(); i++) {
			String fieldName = fieldNames[i];
			TypeInformation typeInfo = rowTypeInfo.getTypeAt(i);
			keyMap.put(fieldName, Tuple3.of(columnFamily, fieldName, typeInfo));
		}
		return this;
	}

	public <T> HBaseTableMapper setMapping(PojoTypeInfo<T> typeInfo, String columnFamily) {
		for (int i = 0; i < typeInfo.getArity(); i++) {
			PojoField pojoField = typeInfo.getPojoFieldAt(i);
			String fieldName = pojoField.getField().getName();
			TypeInformation<?> fieldTypeInfo = pojoField.getTypeInformation();
			keyMap.put(fieldName, Tuple3.of(columnFamily, fieldName, fieldTypeInfo));
		}
		return this;
	}

	public HBaseTableMapper setRowKey(String key, Class clazz) {
		this.rowKey = key;
		this.rowKeyType = TypeExtractor.getForClass(clazz);
		return this;
	}

	public HBaseTableMapper setRowKey(int keyIndex, Class clazz) {
		this.rowKey = String.valueOf(keyIndex);
		this.rowKeyType = TypeExtractor.getForClass(clazz);
		return this;
	}

	public String getRowKey() {
		return rowKey;
	}

	public TypeInformation<?> getRowKeyType() {
		return rowKeyType;
	}

	public Tuple3<byte[], byte[], TypeInformation<?>> getColInfo(String key) {
		Tuple3<String, String, TypeInformation<?>> rowInfo = keyMap.get(key);
		return Tuple3.of(rowInfo.f0.getBytes(), rowInfo.f1.getBytes(), rowInfo.f2);
	}

	public String[] getKeyList() {
		Set<String> keySet = keyMap.keySet();
		String[] keyList = new String[keySet.size()];
		int i = 0;
		for (String key : keySet) {
			keyList[i++] = key;
		}
		return keyList;
	}

	public String getCharset() {
		return charset;
	}

	public void setCharset(String charset) {
		this.charset = charset;
	}

	public static byte[] serialize(TypeInformation<?> typeInfo, Object obj) throws Exception {
		Class clazz = typeInfo.getTypeClass();
		if (byte[].class.equals(clazz)) {
			return (byte[]) obj;
		} else if (String.class.equals(clazz)) {
			return Bytes.toBytes((String) obj);
		} else if (Byte.class.equals(clazz)) {
			return Bytes.toBytes((Byte) obj);
		} else if (Short.class.equals(clazz)) {
			return Bytes.toBytes((Short) obj);
		} else if (Integer.class.equals(clazz)) {
			return Bytes.toBytes((Integer) obj);
		} else if (Long.class.equals(clazz)) {
			return Bytes.toBytes((Long) obj);
		} else if (Float.class.equals(clazz)) {
			return Bytes.toBytes((Float) obj);
		} else if (Double.class.equals(clazz)) {
			return Bytes.toBytes((Double) obj);
		} else if (Boolean.class.equals(clazz)) {
			return Bytes.toBytes((Boolean) obj);
		} else if (Timestamp.class.equals(clazz)) {
			return Bytes.toBytes(((Timestamp) obj).getTime());
		} else if (Date.class.equals(clazz)) {
			return Bytes.toBytes(((Date) obj).getTime());
		} else if (Time.class.equals(clazz)) {
			return Bytes.toBytes(((Time) obj).getTime());
		} else if (BigDecimal.class.equals(clazz)) {
			return Bytes.toBytes((BigDecimal) obj);
		} else if (BigInteger.class.equals(clazz)) {
			return ((BigInteger) obj).toByteArray();
		} else {
			throw new Exception("Unsupported type " + clazz.getName());
		}
	}

}
