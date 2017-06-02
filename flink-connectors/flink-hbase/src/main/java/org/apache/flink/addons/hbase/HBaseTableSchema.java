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

import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Helps to specify an HBase Table's schema.
 */
public class HBaseTableSchema implements Serializable {

	// A Map with key as column family.
	private final Map<String, Map<String, TypeInformation<?>>> familyMap = new LinkedHashMap<>();

	// charset to parse HBase keys and strings. UTF-8 by default.
	private String charset = "UTF-8";

	/**
	 * Adds a column defined by family, qualifier, and type to the table schema.
	 *
	 * @param family    the family name
	 * @param qualifier the qualifier name
	 * @param clazz     the data type of the qualifier
	 */
	void addColumn(String family, String qualifier, Class<?> clazz) {
		Preconditions.checkNotNull(family, "family name");
		Preconditions.checkNotNull(qualifier, "qualifier name");
		Preconditions.checkNotNull(clazz, "class type");
		Map<String, TypeInformation<?>> qualifierMap = this.familyMap.get(family);

		if (!HBaseRowInputFormat.isSupportedType(clazz)) {
			// throw exception
			throw new IllegalArgumentException("Unsupported class type found " + clazz + ". " +
				"Better to use byte[].class and deserialize using user defined scalar functions");
		}

		if (qualifierMap == null) {
			qualifierMap = new LinkedHashMap<>();
		}
		qualifierMap.put(qualifier, TypeExtractor.getForClass(clazz));
		familyMap.put(family, qualifierMap);
	}

	/**
	 * Sets the charset for value strings and HBase identifiers.
	 *
	 * @param charset the charset for value strings and HBase identifiers.
	 */
	void setCharset(String charset) {
		this.charset = charset;
	}

	/**
	 * Returns the names of all registered column families.
	 *
	 * @return The names of all registered column families.
	 */
	String[] getFamilyNames() {
		return this.familyMap.keySet().toArray(new String[this.familyMap.size()]);
	}

	/**
	 * Returns the HBase identifiers of all registered column families.
	 *
	 * @return The HBase identifiers of all registered column families.
	 */
	byte[][] getFamilyKeys() {
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
	String[] getQualifierNames(String family) {
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
	byte[][] getQualifierKeys(String family) {
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
	TypeInformation<?>[] getQualifierTypes(String family) {
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
	Map<String, TypeInformation<?>> getFamilyInfo(String family) {
		return familyMap.get(family);
	}

	/**
	 * Returns the charset for value strings and HBase identifiers.
	 *
	 * @return The charset for value strings and HBase identifiers.
	 */
	String getStringCharset() {
		return this.charset;
	}

}
