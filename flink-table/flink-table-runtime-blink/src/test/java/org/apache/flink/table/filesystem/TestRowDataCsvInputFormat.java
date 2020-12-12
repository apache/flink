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

import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.io.RowCsvInputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.table.utils.PartitionPathUtils;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collectors;

/**
 * The {@link InputFormat} that output {@link RowData}.
 */
public class TestRowDataCsvInputFormat extends RichInputFormat<RowData, FileInputSplit> {

	private final List<String> partitionKeys;
	private final String defaultPartValue;
	private final int[] selectFields;
	private final long limit;
	private final RowCsvInputFormat inputFormat;
	private final List<TypeInformation> fieldTypes;
	private final List<String> fieldNames;
	private final List<DataFormatConverters.DataFormatConverter> csvSelectConverters;
	private final int[] csvFieldMapping;

	private transient Row csvRow;
	private transient GenericRowData row;
	private transient long emitted;

	public TestRowDataCsvInputFormat(
			Path[] paths,
			TableSchema schema,
			List<String> partitionKeys,
			String defaultPartValue,
			int[] selectFields,
			long limit) {
		this.partitionKeys = partitionKeys;
		this.defaultPartValue = defaultPartValue;
		this.selectFields = selectFields;
		this.limit = limit;
		RowTypeInfo rowType = (RowTypeInfo) schema.toRowType();
		this.fieldTypes = Arrays.asList(rowType.getFieldTypes());
		this.fieldNames = Arrays.asList(rowType.getFieldNames());

		List<String> csvFieldNames = fieldNames.stream()
				.filter(name -> !partitionKeys.contains(name)).collect(Collectors.toList());

		List<String> selectFieldNames = Arrays.stream(selectFields)
				.mapToObj(fieldNames::get)
				.collect(Collectors.toList());
		List<String> csvSelectFieldNames = selectFieldNames.stream()
				.filter(name -> !partitionKeys.contains(name)).collect(Collectors.toList());
		List<TypeInformation> csvSelectTypes = csvSelectFieldNames.stream()
				.map(name -> fieldTypes.get(fieldNames.indexOf(name))).collect(Collectors.toList());
		this.csvSelectConverters = csvSelectTypes.stream()
				.map(TypeConversions::fromLegacyInfoToDataType)
				.map(DataFormatConverters::getConverterForDataType)
				.collect(Collectors.toList());
		int[] csvSelectFields = csvSelectFieldNames.stream().mapToInt(csvFieldNames::indexOf).toArray();
		this.inputFormat = new RowCsvInputFormat(
				null, csvSelectTypes.toArray(new TypeInformation[0]), csvSelectFields);
		this.inputFormat.setFilePaths(paths);

		this.csvFieldMapping = csvSelectFieldNames.stream().mapToInt(selectFieldNames::indexOf).toArray();
		this.emitted = 0;
	}

	@Override
	public void configure(Configuration parameters) {
		inputFormat.configure(parameters);
	}

	@Override
	public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
		return inputFormat.getStatistics(cachedStatistics);
	}

	@Override
	public FileInputSplit[] createInputSplits(int minNumSplits) throws IOException {
		return inputFormat.createInputSplits(minNumSplits);
	}

	@Override
	public InputSplitAssigner getInputSplitAssigner(FileInputSplit[] inputSplits) {
		return inputFormat.getInputSplitAssigner(inputSplits);
	}

	@Override
	public void open(FileInputSplit split) throws IOException {
		inputFormat.open(split);
		Path path = split.getPath();
		LinkedHashMap<String, String> partSpec = PartitionPathUtils.extractPartitionSpecFromPath(path);
		this.row = new GenericRowData(selectFields.length);
		for (int i = 0; i < selectFields.length; i++) {
			int selectField = selectFields[i];
			String name = fieldNames.get(selectField);
			if (partitionKeys.contains(name)) {
				String value = partSpec.get(name);
				value = defaultPartValue.equals(value) ? null : value;
				this.row.setField(
						i, convertStringToInternal(value, fieldTypes.get(selectField)));
			}
		}
		this.csvRow = new Row(csvSelectConverters.size());
	}

	private Object convertStringToInternal(String value, TypeInformation type) {
		if (type.equals(Types.INT)) {
			return Integer.parseInt(value);
		} else if (type.equals(Types.LONG)) {
			return Long.parseLong(value);
		} else if (type.equals(Types.STRING)) {
			return StringData.fromString(value);
		} else {
			throw new UnsupportedOperationException("Unsupported partition type: " + type);
		}
	}

	@Override
	public boolean reachedEnd() {
		return emitted >= limit || inputFormat.reachedEnd();
	}

	@Override
	public RowData nextRecord(RowData reuse) throws IOException {
		Row csvRow = inputFormat.nextRecord(this.csvRow);
		if (csvRow == null) {
			return null;
		}
		for (int i = 0; i < csvSelectConverters.size(); i++) {
			row.setField(
					csvFieldMapping[i],
					csvSelectConverters.get(i).toInternal(csvRow.getField(i)));
		}
		emitted++;
		return row;
	}

	@Override
	public void close() throws IOException {
		inputFormat.close();
	}
}
