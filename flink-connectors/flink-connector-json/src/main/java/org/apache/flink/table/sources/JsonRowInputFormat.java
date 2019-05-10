/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.sources;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.json.JsonRowDeserializationSchema;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;

/**
 * InputFormat that reads json files.
 *
 */
@Internal
public class JsonRowInputFormat extends FileInputFormat<Row> implements ResultTypeQueryable<Row> {

	private String[] fieldNames;
	private TypeInformation[] fieldTypes;
	private int[] selectedFields;

	private transient boolean hasLine;
	private transient Scanner sc;
	private transient RowTypeInfo rowType;
	private transient FSDataInputStream inputStream;

	private transient JsonRowDeserializationSchema schema;

	public JsonRowInputFormat(String filePath, String[] fieldNames, TypeInformation[] fieldTypes) {
		super(new Path(filePath));
		this.fieldNames = fieldNames;
		this.fieldTypes = fieldTypes;
		// set default selection mask, i.e., all fields.
		this.selectedFields = new int[fieldNames.length];
		for (int i = 0; i < selectedFields.length; i++) {
			this.selectedFields[i] = i;
		}
	}

	@Override
	public boolean reachedEnd() throws IOException {
		return !hasLine;
	}

	@Override
	public void open(FileInputSplit fileSplit) throws IOException {
		Path path = fileSplit.getPath();
		FileSystem fs = FileSystem.get(path.toUri());
		inputStream = fs.open(path);
		sc = new Scanner(inputStream, StandardCharsets.UTF_8.name());
		hasLine = sc.hasNextLine();

		setRowType();
		schema = new JsonRowDeserializationSchema(this.rowType);
	}

	@Override
	public void close() throws IOException {
		if (null != inputStream) {
			inputStream.close();
		}

		if (null != sc) {
			sc.close();
		}
	}

	@Override
	public Row nextRecord(Row row) throws IOException {
		if (!hasLine) {
			return null;
		}

		String line = sc.nextLine();
		row = schema.deserialize(line.getBytes());

		hasLine = sc.hasNextLine();
		return row;
	}

	@Override
	public TypeInformation<Row> getProducedType() {
		return this.rowType;
	}

	public void selectFields(int... selectedFields) {
		// set field mapping
		if (null != selectedFields) {
			this.selectedFields = selectedFields;
		}

		// adapt result type
		setRowType();
	}

	private void setRowType() {
		if (null == this.rowType) {
			this.rowType = new RowTypeInfo(fieldTypes , fieldNames);
			this.rowType = RowTypeInfo.projectFields(this.rowType, this.selectedFields);
		}
	}
}
