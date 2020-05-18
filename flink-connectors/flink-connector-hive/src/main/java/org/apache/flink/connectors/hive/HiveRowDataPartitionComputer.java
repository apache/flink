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

package org.apache.flink.connectors.hive;

import org.apache.flink.table.catalog.hive.client.HiveShim;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.filesystem.RowDataPartitionComputer;
import org.apache.flink.table.functions.hive.conversion.HiveInspectors;
import org.apache.flink.table.functions.hive.conversion.HiveObjectConversion;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.TypeConversions;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import java.util.Arrays;
import java.util.LinkedHashMap;

/**
 * A {@link RowDataPartitionComputer} that converts Flink objects to Hive objects before computing the partition value strings.
 */
public class HiveRowDataPartitionComputer extends RowDataPartitionComputer {

	private final DataFormatConverters.DataFormatConverter[] partitionConverters;
	private final HiveObjectConversion[] hiveObjectConversions;

	public HiveRowDataPartitionComputer(
			HiveShim hiveShim,
			String defaultPartValue,
			String[] columnNames,
			DataType[] columnTypes,
			String[] partitionColumns) {
		super(defaultPartValue, columnNames, columnTypes, partitionColumns);
		this.partitionConverters = Arrays.stream(partitionTypes)
				.map(TypeConversions::fromLogicalToDataType)
				.map(DataFormatConverters::getConverterForDataType)
				.toArray(DataFormatConverters.DataFormatConverter[]::new);
		this.hiveObjectConversions = new HiveObjectConversion[partitionIndexes.length];
		for (int i = 0; i < hiveObjectConversions.length; i++) {
			DataType partColType = columnTypes[partitionIndexes[i]];
			ObjectInspector objectInspector = HiveInspectors.getObjectInspector(partColType);
			hiveObjectConversions[i] = HiveInspectors.getConversion(objectInspector, partColType.getLogicalType(), hiveShim);
		}
	}

	@Override
	public LinkedHashMap<String, String> generatePartValues(RowData in) {
		LinkedHashMap<String, String> partSpec = new LinkedHashMap<>();

		for (int i = 0; i < partitionIndexes.length; i++) {
			Object field = partitionConverters[i].toExternal(in, partitionIndexes[i]);
			String partitionValue = field != null ? hiveObjectConversions[i].toHiveObject(field).toString() : null;
			if (StringUtils.isEmpty(partitionValue)) {
				partitionValue = defaultPartValue;
			}
			partSpec.put(partitionColumns[i], partitionValue);
		}
		return partSpec;
	}
}
