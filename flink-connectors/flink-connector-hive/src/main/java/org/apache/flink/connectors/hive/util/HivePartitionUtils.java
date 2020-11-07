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

package org.apache.flink.connectors.hive.util;

import org.apache.flink.connectors.hive.FlinkHiveException;
import org.apache.flink.table.catalog.hive.client.HiveShim;
import org.apache.flink.table.functions.hive.conversion.HiveInspectors;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import java.sql.Date;
import java.sql.Timestamp;

/**
 * Utils to load hive partitions from HiveMetaStore.
 */
public class HivePartitionUtils {

	public static Object restorePartitionValueFromType(HiveShim shim, String valStr, DataType type) {
		LogicalTypeRoot typeRoot = type.getLogicalType().getTypeRoot();
		//note: it's not a complete list ofr partition key types that Hive support, we may need add more later.
		switch (typeRoot) {
			case CHAR:
			case VARCHAR:
				return valStr;
			case BOOLEAN:
				return Boolean.parseBoolean(valStr);
			case TINYINT:
				return Integer.valueOf(valStr).byteValue();
			case SMALLINT:
				return Short.valueOf(valStr);
			case INTEGER:
				return Integer.valueOf(valStr);
			case BIGINT:
				return Long.valueOf(valStr);
			case FLOAT:
				return Float.valueOf(valStr);
			case DOUBLE:
				return Double.valueOf(valStr);
			case DATE:
				return HiveInspectors.toFlinkObject(
						HiveInspectors.getObjectInspector(type),
						shim.toHiveDate(Date.valueOf(valStr)),
						shim);
			case TIMESTAMP_WITHOUT_TIME_ZONE:
				return HiveInspectors.toFlinkObject(
						HiveInspectors.getObjectInspector(type),
						shim.toHiveTimestamp(Timestamp.valueOf(valStr)),
						shim);
			default:
				break;
		}
		throw new FlinkHiveException(
				new IllegalArgumentException(String.format("Can not convert %s to type %s for partition value", valStr, type)));
	}
}
