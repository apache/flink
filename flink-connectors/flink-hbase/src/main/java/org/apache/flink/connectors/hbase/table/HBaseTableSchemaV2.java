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
import org.apache.flink.connectors.hbase.util.HBaseTypeUtils;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.stream.Collectors;

/**
 * The difference between {@link HBaseTableSchema} is this HBaseTableSchemaV2 contains a RowKey field which is HBase's rowKey object.
 * Note: do not support string type rowKey using different charset against string type qualifiers.
 */
public class HBaseTableSchemaV2 implements Serializable {

	// rowKey info
	private String rowKeyName;
	private TypeInformation rowKeyType;

	// column family(s) info
	private HBaseTableSchema familySchema;

	protected HBaseTableSchemaV2(String rowKeyName, TypeInformation rowKeyType, HBaseTableSchema familySchema) {
		this.rowKeyName = rowKeyName;
		this.rowKeyType = rowKeyType;
		this.familySchema = familySchema;
	}

	public String getRowKeyName() {
		return rowKeyName;
	}

	public TypeInformation getRowKeyType() {
		return rowKeyType;
	}

	public HBaseTableSchema getFamilySchema() {
		return familySchema;
	}

	@Override
	public String toString() {
		return "HBaseTableSchemaV2{" +
			" rowKeyName='" + rowKeyName + '\'' +
			", rowKeyType=" + rowKeyType +
			", charset=" + familySchema.getStringCharset() +
			", familySchema=" +
			familySchema.getFlatStringQualifiers().stream().map(t -> t.f0 + ":" + t.f1 + "|" + t.f2).collect(Collectors.joining(", ")) + '}';
	}

	/**
	 * Helps to build a {@link HBaseTableSchemaV2}.
	 */
	public static class Builder {

		// rowKey info
		String rowKeyName;
		TypeInformation rowKeyType;

		// column family(s) info
		HBaseTableSchema familySchema;

		public Builder(String rowKeyName, TypeInformation rowKeyType) {
			Preconditions.checkNotNull(rowKeyName, "rowKey name");
			if (!HBaseTypeUtils.isSupportedType(rowKeyType)) {
				// throw exception
				throw new IllegalArgumentException("Unsupported row key type found " + rowKeyType + ". " +
					"Better to use byte[].class and deserialize using user defined scalar functions");
			}

			this.rowKeyName = rowKeyName;
			this.rowKeyType = rowKeyType;
			this.familySchema = new HBaseTableSchema();
		}

		public Builder addColumn(String family, String qualifier, TypeInformation<?> type) {
			familySchema.addColumn(family, qualifier, type);
			return this;
		}

		public Builder setCharset(String charset) {
			// let the familySchema keep the charset info for now, also using it for string type row key.
			this.familySchema.setCharset(charset);
			return this;
		}

		public HBaseTableSchemaV2 build() {
			return new HBaseTableSchemaV2(rowKeyName, rowKeyType, familySchema);
		}
	}
}
