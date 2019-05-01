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

package org.apache.flink.table.catalog.hive;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Shared util for catalogs backed by Hive-metastore.
 */
public class HiveCatalogBaseUtil {

	/**
	 * Create a Flink's TableSchema from Hive table's columns and partition keys.
	 *
	 * @param cols columns of the Hive table
	 * @param partitionKeys partition keys of the Hive table
	 * @return a Flink TableSchema
	 */
	public static TableSchema createTableSchema(List<FieldSchema> cols, List<FieldSchema> partitionKeys) {
		List<FieldSchema> allCols = new ArrayList<>(cols);
		allCols.addAll(partitionKeys);

		String[] colNames = new String[allCols.size()];
		TypeInformation[] colTypes = new TypeInformation[allCols.size()];

		for (int i = 0; i < allCols.size(); i++) {
			FieldSchema fs = allCols.get(i);

			colNames[i] = fs.getName();
			colTypes[i] = HiveTypeUtil.toFlinkType(TypeInfoUtils.getTypeInfoFromTypeString(fs.getType()));
		}

		return new TableSchema(colNames, colTypes);
	}
}
