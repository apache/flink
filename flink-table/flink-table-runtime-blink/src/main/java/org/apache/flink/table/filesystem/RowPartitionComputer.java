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

package org.apache.flink.table.filesystem;

import org.apache.flink.annotation.Internal;
import org.apache.flink.types.Row;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * {@link PartitionComputer} for {@link Row}.
 */
@Internal
public class RowPartitionComputer implements PartitionComputer<Row> {

	private static final long serialVersionUID = 1L;

	protected final String defaultPartValue;
	protected final String[] partitionColumns;
	private final int[] nonPartitionIndexes;
	protected final int[] partitionIndexes;

	public RowPartitionComputer(String defaultPartValue, String[] columnNames, String[] partitionColumns) {
		this.defaultPartValue = defaultPartValue;
		this.partitionColumns = partitionColumns;
		List<String> columnList = Arrays.asList(columnNames);
		this.partitionIndexes = Arrays.stream(partitionColumns).mapToInt(columnList::indexOf).toArray();
		List<Integer> partitionIndexList = Arrays.stream(partitionIndexes).boxed().collect(Collectors.toList());
		this.nonPartitionIndexes = IntStream.range(0, columnNames.length)
				.filter(c -> !partitionIndexList.contains(c))
				.toArray();
	}

	@Override
	public LinkedHashMap<String, String> generatePartValues(Row in) throws Exception {
		LinkedHashMap<String, String> partSpec = new LinkedHashMap<>();

		for (int i = 0; i < partitionIndexes.length; i++) {
			int index = partitionIndexes[i];
			Object field = in.getField(index);
			String partitionValue = field != null ? field.toString() : null;
			if (partitionValue == null || "".equals(partitionValue)) {
				partitionValue = defaultPartValue;
			}
			partSpec.put(partitionColumns[i], partitionValue);
		}
		return partSpec;
	}

	@Override
	public Row projectColumnsToWrite(Row in) throws Exception {
		return partitionIndexes.length == 0 ? in : Row.project(in, nonPartitionIndexes);
	}
}
