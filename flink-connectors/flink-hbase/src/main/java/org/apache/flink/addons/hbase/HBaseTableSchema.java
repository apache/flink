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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.util.Preconditions;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Time;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.sql.Date;

/**
 * Helps to specify an HBase Table's schema
 */
public class HBaseTableSchema implements Serializable {

	// A Map with key as column family.
	private final Map<String, List<Pair<String, TypeInformation<?>>>> familyMap =
		new HashMap<>();

	// Allowed types. This may change.
	// TODO : Check if the Date type should be the one in java.util or the one in java.sql
	private static Class[] CLASS_TYPES = {
		Integer.class, Short.class, Float.class, Long.class, String.class, Byte.class, Boolean.class, Double.class, BigInteger.class, BigDecimal.class, Date.class, Time.class, byte[].class
	};
	/**
	 * Allows specifying the family and qualifier name along with the data type of the qualifier for an HBase table
	 *
	 * @param family    the family name
	 * @param qualifier the qualifier name
	 * @param clazz     the data type of the qualifier
	 */
	public void addColumn(String family, String qualifier, Class<?> clazz) {
		Preconditions.checkNotNull(family, "family name");
		Preconditions.checkNotNull(qualifier, "qualifier name");
		Preconditions.checkNotNull(clazz, "class type");
		List<Pair<String, TypeInformation<?>>> list = this.familyMap.get(family);
		if (list == null) {
			list = new ArrayList<>();
		}
		boolean found = false;
		for (Class classType : CLASS_TYPES) {
			if (classType == clazz) {
				found = true;
				break;
			}
		}
		if (!found) {
			// throw exception
			throw new IllegalArgumentException("Unsupported class type found " + clazz+". Better to use byte[].class and deserialize using user defined scalar functions");
		}
		list.add(new Pair<String, TypeInformation<?>>(qualifier, TypeExtractor.getForClass(clazz)));
		familyMap.put(family, list);
	}

	public String[] getFamilyNames() {
		return this.familyMap.keySet().toArray(new String[this.familyMap.size()]);
	}

	public String[] getQualifierNames(String family) {
		List<Pair<String, TypeInformation<?>>> colDetails = familyMap.get(family);
		String[] qualifierNames = new String[colDetails.size()];
		int i = 0;
		for (Pair<String, TypeInformation<?>> pair : colDetails) {
			qualifierNames[i] = pair.getFirst();
			i++;
		}
		return qualifierNames;
	}

	public TypeInformation<?>[] getQualifierTypes(String family) {
		List<Pair<String, TypeInformation<?>>> colDetails = familyMap.get(family);
		TypeInformation<?>[] typeInformations = new TypeInformation[colDetails.size()];
		int i = 0;
		for (Pair<String, TypeInformation<?>> pair : colDetails) {
			typeInformations[i] = pair.getSecond();
			i++;
		}
		return typeInformations;
	}

	public List<Pair<String, TypeInformation<?>>> getFamilyInfo(String family) {
		return familyMap.get(family);
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
			} else if (typeInfo.getTypeClass() == Time.class) {
				return new Time(Bytes.toLong(value));
			}
		}
		return value;
	}
}
