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

package org.apache.flink.connectors.hbase.table;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.connectors.hbase.util.HBaseTypeUtils;
import org.apache.flink.util.Preconditions;

import org.apache.hadoop.hbase.util.Bytes;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Helps to specify an HBase Table's schema.
 */
public class HBaseTableSchema implements Serializable {

	// A Map with key as column family.
	private final Map<String, Map<String, TypeInformation<?>>> familyMap = new LinkedHashMap<>();

	// charset to parse HBase keys and strings. UTF-8 by default.
	private String charset = "UTF-8";

	// count total qualifier number
	private int totalQualifiers = 0;

	/**
	 * Adds a column defined by family, qualifier, and type to the table schema.
	 *
	 * @param family    the family name
	 * @param qualifier the qualifier name
	 * @param clazz     the data type of the qualifier
	 */
	@Deprecated
	public void addColumn(String family, String qualifier, Class<?> clazz) {
		if (!HBaseTypeUtils.isSupportedType(clazz)) {
			// throw exception
			throw new IllegalArgumentException("Unsupported class type found " + clazz + ". " +
				"Better to use byte[].class and deserialize using user defined scalar functions");
		}
		// redirect to new method
		addColumn(family, qualifier, TypeExtractor.getForClass(clazz));
	}

	/**
	 * Adds a column defined by family, qualifier, and type to the table schema.
	 *
	 * @param family    the family name
	 * @param qualifier the qualifier name
	 * @param type the data type info of the qualifier
	 */
	public void addColumn(String family, String qualifier, TypeInformation<?> type) {
		Preconditions.checkNotNull(family, "family name");
		Preconditions.checkNotNull(qualifier, "qualifier name");
		Preconditions.checkNotNull(type, "qualifier type");
		Map<String, TypeInformation<?>> qualifierMap = this.familyMap.get(family);

		if (!HBaseTypeUtils.isSupportedType(type)) {
			// throw exception
			throw new IllegalArgumentException("Unsupported type found " + type + ". " +
				"Better to use byte[].class and deserialize using user defined scalar functions");
		}

		if (qualifierMap == null) {
			qualifierMap = new LinkedHashMap<>();
		}

		// validate qualifier name's uniqueness
		if (qualifierMap.containsKey(qualifier)) {
			throw new IllegalArgumentException("qualifier[" + qualifier + "] in column[" + family + "] already exists!");
		}
		qualifierMap.put(qualifier, type);
		familyMap.put(family, qualifierMap);
		totalQualifiers++;
	}

	/**
	 * Sets the charset for value strings and HBase identifiers.
	 *
	 * @param charset the charset for value strings and HBase identifiers.
	 */
	public void setCharset(String charset) {
		this.charset = charset;
	}

	/**
	 * Returns the names of all registered column families.
	 *
	 * @return The names of all registered column families.
	 */
	public String[] getFamilyNames() {
		return this.familyMap.keySet().toArray(new String[this.familyMap.size()]);
	}

	/**
	 * Returns the HBase identifiers of all registered column families.
	 *
	 * @return The HBase identifiers of all registered column families.
	 */
	public byte[][] getFamilyKeys() {
		Charset c = Charset.forName(charset);

		byte[][] familyKeys = new byte[this.familyMap.size()][];
		int i = 0;
		for (String name : this.familyMap.keySet()) {
			familyKeys[i++] = name.getBytes(c);
		}
		return familyKeys;
	}

	/**
	 * Returns the names of all registered column qualifiers of a specific column family.
	 *
	 * @param family The name of the column family for which the column qualifier names are returned.
	 * @return The names of all registered column qualifiers of a specific column family.
	 */
	public String[] getQualifierNames(String family) {
		Map<String, TypeInformation<?>> qualifierMap = familyMap.get(family);

		if (qualifierMap == null) {
			throw new IllegalArgumentException("Family " + family + " does not exist in schema.");
		}

		String[] qualifierNames = new String[qualifierMap.size()];
		int i = 0;
		for (String qualifier: qualifierMap.keySet()) {
			qualifierNames[i] = qualifier;
			i++;
		}
		return qualifierNames;
	}

	/**
	 * Returns the HBase identifiers of all registered column qualifiers for a specific column family.
	 *
	 * @param family The name of the column family for which the column qualifier identifiers are returned.
	 * @return The HBase identifiers of all registered column qualifiers for a specific column family.
	 */
	public byte[][] getQualifierKeys(String family) {
		Map<String, TypeInformation<?>> qualifierMap = familyMap.get(family);

		if (qualifierMap == null) {
			throw new IllegalArgumentException("Family " + family + " does not exist in schema.");
		}
		Charset c = Charset.forName(charset);

		byte[][] qualifierKeys = new byte[qualifierMap.size()][];
		int i = 0;
		for (String name : qualifierMap.keySet()) {
			qualifierKeys[i++] = name.getBytes(c);
		}
		return qualifierKeys;
	}

	/**
	 * Returns the types of all registered column qualifiers of a specific column family.
	 *
	 * @param family The name of the column family for which the column qualifier types are returned.
	 * @return The types of all registered column qualifiers of a specific column family.
	 */
	public TypeInformation<?>[] getQualifierTypes(String family) {
		Map<String, TypeInformation<?>> qualifierMap = familyMap.get(family);

		if (qualifierMap == null) {
			throw new IllegalArgumentException("Family " + family + " does not exist in schema.");
		}

		TypeInformation<?>[] typeInformation = new TypeInformation[qualifierMap.size()];
		int i = 0;
		for (TypeInformation<?> typeInfo : qualifierMap.values()) {
			typeInformation[i] = typeInfo;
			i++;
		}
		return typeInformation;
	}

	/**
	 * Returns the names and types of all registered column qualifiers of a specific column family.
	 *
	 * @param family The name of the column family for which the column qualifier names and types are returned.
	 * @return The names and types of all registered column qualifiers of a specific column family.
	 */
	public Map<String, TypeInformation<?>> getFamilyInfo(String family) {
		return familyMap.get(family);
	}

	/**
	 * Returns the charset for value strings and HBase identifiers.
	 *
	 * @return The charset for value strings and HBase identifiers.
	 */
	public String getStringCharset() {
		return this.charset;
	}

	public int getTotalQualifiers() {
		return totalQualifiers;
	}

	/**
	 * A helper method returns a flat qualifiers list in defining order.
	 * Qualifier info is a Tuple3 representation:
	 * (byte[] columnFamily, byte[] qualifier, TypeInformation typeInfo)
	 */
	public List<Tuple3<byte[], byte[], TypeInformation<?>>> getFlatByteQualifiers() {
		List<Tuple3<byte[], byte[], TypeInformation<?>>> qualifierList = new ArrayList<>();

		for (String family : getFamilyNames()) {
			byte[] columnBytes = Bytes.toBytes(family);
			String[] qualifierNames = getQualifierNames(family);
			TypeInformation[] qualifierTypes = getQualifierTypes(family);
			for (int idx = 0; idx < qualifierNames.length; idx++) {
				qualifierList.add(new Tuple3(columnBytes, Bytes.toBytes(qualifierNames[idx]), qualifierTypes[idx]));
			}
		}
		return qualifierList;
	}

	/**
	 * A helper method returns a flat qualifiers list in defining order.
	 * Qualifier info is a Tuple3 representation:
	 * (String columnFamily, String qualifier, TypeInformation typeInfo)
	 */
	public List<Tuple3<String, String, TypeInformation<?>>> getFlatStringQualifiers() {
		List<Tuple3<String, String, TypeInformation<?>>> qualifierList = new ArrayList<>();

		for (String family : getFamilyNames()) {
			String[] qualifierNames = getQualifierNames(family);
			TypeInformation[] qualifierTypes = getQualifierTypes(family);
			for (int idx = 0; idx < qualifierNames.length; idx++) {
				qualifierList.add(new Tuple3(family, qualifierNames[idx], qualifierTypes[idx]));
			}
		}
		return qualifierList;
	}
}
