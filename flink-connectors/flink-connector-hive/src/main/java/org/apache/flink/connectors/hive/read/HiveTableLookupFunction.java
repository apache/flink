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

package org.apache.flink.connectors.hive.read;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connectors.hive.FlinkHiveException;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Lookup table function for Hive tables.
 */
public class HiveTableLookupFunction extends TableFunction<RowData> {

	private static final long serialVersionUID = 1L;

	private final HiveTableInputFormat inputFormat;
	// indices of lookup columns in the record returned by input format
	private final int[] lookupCols;
	private transient Map<RowData, List<RowData>> cache;
	// timestamp when cache expires
	private transient long cacheExpire;
	private final Duration cacheTTL = Duration.ofHours(1);

	public HiveTableLookupFunction(HiveTableInputFormat inputFormat, String[] lookupKeys) {
		lookupCols = new int[lookupKeys.length];
		String[] allFields = inputFormat.getFieldNames();
		Map<String, Integer> nameToIndex = IntStream.range(0, allFields.length).boxed().collect(
				Collectors.toMap(i -> allFields[i], i -> i));
		List<Integer> selectedIndices = Arrays.stream(inputFormat.getSelectedFields()).boxed().collect(Collectors.toList());
		for (int i = 0; i < lookupKeys.length; i++) {
			Integer index = nameToIndex.get(lookupKeys[i]);
			Preconditions.checkArgument(index != null, "Lookup keys %s not found in table schema", Arrays.toString(lookupKeys));
			index = selectedIndices.indexOf(index);
			Preconditions.checkArgument(index >= 0, "Lookup keys %s not selected", Arrays.toString(lookupKeys));
			lookupCols[i] = index;
		}
		this.inputFormat = inputFormat;
	}

	@Override
	public TypeInformation<RowData> getResultType() {
		String[] allNames = inputFormat.getFieldNames();
		DataType[] allTypes = inputFormat.getFieldTypes();
		int[] selected = inputFormat.getSelectedFields();
		return new RowDataTypeInfo(
				Arrays.stream(selected).mapToObj(i -> allTypes[i].getLogicalType()).toArray(LogicalType[]::new),
				Arrays.stream(selected).mapToObj(i -> allNames[i]).toArray(String[]::new));
	}

	@Override
	public void open(FunctionContext context) throws Exception {
		super.open(context);
		cache = new HashMap<>();
		cacheExpire = -1;
	}

	public void eval(Object... values) {
		Preconditions.checkArgument(values.length == lookupCols.length, "Number of values and lookup keys mismatch");
		reloadCache();
		RowData probeKey = GenericRowData.of(values);
		List<RowData> matchedRows = cache.get(probeKey);
		if (matchedRows != null) {
			for (RowData matchedRow : matchedRows) {
				collect(matchedRow);
			}
		}
	}

	private void reloadCache() {
		if (cacheExpire > System.currentTimeMillis()) {
			return;
		}
		cache.clear();
		try {
			HiveTableInputSplit[] inputSplits = inputFormat.createInputSplits(1);
			GenericRowData reuse = new GenericRowData(inputFormat.getSelectedFields().length);
			for (HiveTableInputSplit split : inputSplits) {
				inputFormat.open(split);
				while (!inputFormat.reachedEnd()) {
					GenericRowData row = (GenericRowData) inputFormat.nextRecord(reuse);
					GenericRowData key = extractKey(row);
					List<RowData> rows = cache.computeIfAbsent(key, k -> new ArrayList<>());
					rows.add(copyReference(row));
				}
				inputFormat.close();
			}
			cacheExpire = System.currentTimeMillis() + cacheTTL.toMillis();
		} catch (IOException e) {
			throw new FlinkHiveException("Failed to load hive table into cache", e);
		}
	}

	private GenericRowData extractKey(GenericRowData row) {
		GenericRowData key = new GenericRowData(lookupCols.length);
		for (int i = 0; i < lookupCols.length; i++) {
			key.setField(i, row.getField(lookupCols[i]));
		}
		return key;
	}

	private static GenericRowData copyReference(GenericRowData rowData) {
		GenericRowData res = new GenericRowData(rowData.getArity());
		res.setRowKind(rowData.getRowKind());
		for (int i = 0; i < rowData.getArity(); i++) {
			res.setField(i, rowData.getField(i));
		}
		return res;
	}
}
