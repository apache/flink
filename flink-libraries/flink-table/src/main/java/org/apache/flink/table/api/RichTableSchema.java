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

package org.apache.flink.table.api;

import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.types.DataType;
import org.apache.flink.table.api.types.DataTypes;
import org.apache.flink.table.api.types.InternalType;
import org.apache.flink.table.api.types.RowType;
import org.apache.flink.table.api.types.TypeConverters;
import org.apache.flink.table.sources.IndexKey;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A RichTableSchema represents a Table's structure.
 */
public class RichTableSchema implements Serializable {

	private final String[] columnNames;
	private final InternalType[] columnTypes;
	private final boolean[] nullables;

	// TODO should introduce table constraints later.
	private final List<String> primaryKeys = new ArrayList<>();
	private final List<List<String>> uniqueKeys = new ArrayList<>();

	// index info, may include unique index or non-unique index or both.
	private final List<Index> indexes = new ArrayList<>();

	private final List<String> headerFields = new ArrayList<>();

	/**
	 * Creates a new RichTableSchema with column names and column types. All the columns
	 * exist in the origin table, no computed column exists.
	 *
	 * @param columnNames column names
	 * @param columnTypes column names
	 */
	public RichTableSchema(String[] columnNames, InternalType[] columnTypes) {
		boolean[] nullables = new boolean[columnNames.length];
		for (int i = 0; i < nullables.length; i++) {
			nullables[i] = true;
		}
		this.columnNames = columnNames;
		this.columnTypes = columnTypes;
		this.nullables = nullables;
	}

	public RichTableSchema(String[] columnNames, InternalType[] columnTypes, boolean[] nullables) {
		this.columnNames = columnNames;
		this.columnTypes = columnTypes;
		this.nullables = nullables;
	}

	/**
	 * Returns the header field names which is in the properties of source record.
	 */
	public List<String> getHeaderFields() {
		return headerFields;
	}

	/**
	 * Set the header fields which is in the properties of source record.
	 *
	 * @param fields header field names
	 */
	public void setHeaderFields(List<String> fields) {
		checkArgument(headerFields.size() == 0, "header fields has been set");
		Set<String> columnNames = new HashSet<>();
		columnNames.addAll(Arrays.asList(getColumnNames()));
		for (String field : fields) {
			if (!columnNames.contains(field)) {
				throw new IllegalArgumentException("The HEADER column field '" + field +
					"' is not in the table schema");
			}
			headerFields.add(field);
		}
	}

	/**
	 * Set the primary keys Maybe one or more keys are combined to be a primary key.
	 *
	 * @param keys one or more combined primary key
	 */
	public void setPrimaryKey(String... keys) {
		checkArgument(primaryKeys.size() == 0, "primary key has been set");
		Set<String> columnNames = new HashSet<>();
		columnNames.addAll(Arrays.asList(getColumnNames()));
		for (String key : keys) {
			if (!columnNames.contains(key)) {
				throw new IllegalArgumentException("The primary key '" + key + "' is not in the table schema");
			}
			primaryKeys.add(key);
		}
	}

	/**
	 * Set the unique keys Maybe one or more keys are combined to be a unique key.
	 */
	public void setUniqueKeys(List<List<String>> uniqueKeys) {
		checkArgument(this.uniqueKeys.size() == 0, "unique key has been set");
		Set<String> columnNames = new HashSet<>();
		columnNames.addAll(Arrays.asList(getColumnNames()));

		for (List<String> uk : uniqueKeys) {
			for (String key : uk) {
				if (!columnNames.contains(key)) {
					throw new IllegalArgumentException("The unique key '" + key + "' is not in the table schema");
				}
			}
			this.uniqueKeys.add(uk);
		}
	}

	/**
	 * Set the indexes.
	 */
	public void setIndexes(List<Index> keys) {
		checkArgument(indexes.size() == 0, "indexes has been set");
		Set<String> columnNames = new HashSet<>();
		columnNames.addAll(Arrays.asList(getColumnNames()));

		for (Index index : keys) {
			addIndex(index, columnNames);
		}
	}

	/**
	 * Set single index.
	 */
	public void addSingleIndex(Index index) {
		Set<String> columnNames = new HashSet<>();
		columnNames.addAll(Arrays.asList(getColumnNames()));
		addIndex(index, columnNames);
	}

	protected void addIndex(Index index, Set<String> columnNames) {
		for (String key : index.keyList) {
			if (!columnNames.contains(key)) {
				throw new IllegalArgumentException("The index key '" + key + "' is not in the table schema");
			}
		}
		indexes.add(index);
	}

	/**
	 * Returns the primary keys Maybe one or more keys are combined to be a primary key.
	 */
	public List<String> getPrimaryKeys() {
		return primaryKeys;
	}

	public List<List<String>> getUniqueKeys() {
		return uniqueKeys;
	}

	/**
	 * Returns original indexes defined in the table.
	 */
	public List<Index> getIndexes() {
		return indexes;
	}

	/**
	 * Returns all deduced indexes defined in the table, include primary key/ unique key/ indexes.
	 */
	public List<Index> deduceAllIndexes() {
		List<Index> indexList = new ArrayList<>();
		if (null != primaryKeys && primaryKeys.size() > 0) {
			indexList.add(new UniqueIndex(primaryKeys));
		}
		for (List<String> uk: uniqueKeys) {
			indexList.add(new UniqueIndex((uk)));
		}
		for (Index idx : indexes) {
			indexList.add(idx);
		}
		return indexList;
	}

	/**
	 * Utility method to convert sql Index to table api IndexKey(s).
	 */
	public List<IndexKey> toIndexKeys() {
		List<Index> indexes = deduceAllIndexes();
		List<IndexKey> indexKeys = new ArrayList<>();
		for (Index index : indexes) {
			indexKeys.add(index.toIndexKey(this));
		}
		return indexKeys;
	}

	/**
	 * Returns the final result type info of this table.
	 * This type info including the computed columns (if exist) and exclude proctime field.
	 */
	public RowType getResultType() {
		return new RowType(getColumnTypes(), getColumnNames());
	}

	/**
	 * Returns the final result type info of this table.
	 * This type info including the computed columns (if exist) and exclude proctime field.
	 */
	public DataType getResultRowType() {
		return DataTypes.createRowType(getColumnTypes(), getColumnNames());
	}

	public RowTypeInfo getResultTypeInfo() {
		return (RowTypeInfo) TypeConverters.createExternalTypeInfoFromDataType(getResultRowType());
	}

	public String[] getColumnNames() {
		return columnNames;
	}

	public InternalType[] getColumnTypes() {
		return columnTypes;
	}

	public boolean[] getNullables() {
		return nullables;
	}

	/**
	 * Describe the structure of an UniqueIndex.
	 */
	public static class UniqueIndex extends Index {

		public UniqueIndex(List<String> keyList) {
			super(true, keyList);
		}
	}

	/**
	 * Describe the structure of an Index.
	 */
	public static class Index {
		public final boolean unique;
		public final List<String> keyList;

		public Index(boolean unique, List<String> keyList) {
			assert keyList != null;
			this.unique = unique;
			this.keyList = keyList;
		}

		// Convert this Index to internal IndexKey.
		public IndexKey toIndexKey(RichTableSchema schema) {
			assert null != schema;
			String[] fieldNames = schema.getColumnNames();
			int[] indexes = new int[keyList.size()];
			for (int i = 0; i < keyList.size(); i++) {
				int idx = findIndex(keyList.get(i), fieldNames);
				indexes[i] = idx;
			}
			return IndexKey.of(unique, indexes);
		}

		int findIndex(String column, String[] fieldNames) {
			for (int j = 0; j < fieldNames.length; j++) {
				if (fieldNames[j].equals(column)) {
					return j;
				}
			}
			return -1;
		}
	}
}
