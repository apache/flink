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

import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Preconditions;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Date;

/**
 * Helps to specify an HBase Table's schema
 */
public class HBaseTableSchema implements Serializable {

	// A Map with key as column family.
	private final Map<String, List<Pair>> familyMap =
		new HashMap<String, List<Pair>>();

	// Allowed types. This may change.
	// TODO : Check if the Date type should be the one in java.util or the one in java.sql
	private static Class[] CLASS_TYPES = {
		Integer.class, Short.class, Float.class, Long.class, String.class, Byte.class, Boolean.class, Double.class, BigInteger.class, BigDecimal.class, Date.class
	};
	private static byte[] EMPTY_BYTE_ARRAY = new byte[0];
	public void addColumns(String family, String qualifier, TypeInformation<?> type) {
		Preconditions.checkNotNull(family, "family name");
		Preconditions.checkNotNull(family, "qualifier name");
		Preconditions.checkNotNull(type, "type name");
		List<Pair> list = this.familyMap.get(family);
		if (list == null) {
			list = new ArrayList<Pair>();
		}
		boolean found = false;
		for(Class classType : CLASS_TYPES) {
			if(classType == type.getTypeClass()) {
				found = true;
				break;
			}
		}
		if(!found) {
			// by default it will be byte[] type only
			type = BasicArrayTypeInfo.BYTE_ARRAY_TYPE_INFO;
		}
		list.add(new Pair(qualifier, type));
		familyMap.put(family, list);
	}

	public Map<String, List<Pair>> getFamilyMap() {
		return this.familyMap;
	}

	public Object deserialize(byte[] value, TypeInformation<?> typeInfo) {
		if (typeInfo.isBasicType()) {
			if (typeInfo.getTypeClass() == Integer.class) {
				return Bytes.toInt(value);
			} else if (typeInfo.getTypeClass() == Short.class) {
				return Bytes.toShort(value);
			} else if (typeInfo.getTypeClass() == Float.class) {
				return Bytes.toFloat(value);
			} else if (typeInfo.getTypeClass() == Long.class) {
				return Bytes.toLong(value);
			} else if (typeInfo.getTypeClass() == String.class) {
				return Bytes.toString(value);
			} else if (typeInfo.getTypeClass() == Byte.class) {
				return value[0];
			} else if (typeInfo.getTypeClass() == Boolean.class) {
				return Bytes.toBoolean(value);
			} else if (typeInfo.getTypeClass() == Double.class) {
				return Bytes.toDouble(value);
			} else if (typeInfo.getTypeClass() == BigInteger.class) {
				return new BigInteger(value);
			} else if (typeInfo.getTypeClass() == BigDecimal.class) {
				return Bytes.toBigDecimal(value);
			} else if (typeInfo.getTypeClass() == Date.class) {
				return new Date(Bytes.toLong(value));
			}
		}
		return value;
	}

	public Object deserializeNull(TypeInformation<?> typeInfo) {
		// TODO : this may need better handling.
		if(typeInfo.getTypeClass() ==  Integer.class) {
			return Integer.MIN_VALUE;
		} else if(typeInfo.getTypeClass() == Short.class) {
			return Short.MIN_VALUE;
		} else if(typeInfo.getTypeClass() == Float.class) {
			return Float.MIN_VALUE;
		} else if(typeInfo.getTypeClass() == Long.class) {
			return Long.MIN_VALUE;
		} else if(typeInfo.getTypeClass() == String.class) {
			return "NULL";
		} else if(typeInfo.getTypeClass() == Byte.class) {
			return Byte.MIN_VALUE;
		} else if(typeInfo.getTypeClass() == Boolean.class) {
			return Boolean.FALSE;
		} else if(typeInfo.getTypeClass() == Double.class) {
			return Double.MIN_VALUE;
		} else if(typeInfo.getTypeClass() == BigInteger.class) {
			return new BigInteger(Bytes.toBytes(Integer.MIN_VALUE));
		} else if(typeInfo.getTypeClass() == BigDecimal.class) {
			return Bytes.toBigDecimal(Bytes.toBytes(Double.MIN_VALUE));
		} else if(typeInfo.getTypeClass() == Date.class) {
			return new Date(Bytes.toLong(Bytes.toBytes(Long.MIN_VALUE)));
		}
		// Return a byte[]
		return EMPTY_BYTE_ARRAY;
	}

}
