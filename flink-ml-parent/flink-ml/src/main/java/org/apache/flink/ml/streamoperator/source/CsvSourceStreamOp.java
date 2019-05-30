/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.ml.streamoperator.source;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.ml.common.MLSession;
import org.apache.flink.ml.common.utils.RowTypeDataStream;
import org.apache.flink.ml.common.utils.TableUtil;
import org.apache.flink.ml.io.AnnotationUtils;
import org.apache.flink.ml.io.IONameAnnotation;
import org.apache.flink.ml.io.http.HttpRowCsvInputFormat;
import org.apache.flink.ml.io.utils.CsvUtil;
import org.apache.flink.ml.params.Params;
import org.apache.flink.ml.params.io.CsvSourceParams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.types.Row;

import java.net.URL;

/**
 * Streaming source which support CSV data.
 */
@IONameAnnotation(name = "csv")
public final class CsvSourceStreamOp extends BaseSourceStreamOp <CsvSourceStreamOp>
	implements CsvSourceParams <CsvSourceStreamOp> {

	public CsvSourceStreamOp() {
		this(new Params());
	}

	public CsvSourceStreamOp(Params params) {
		super(AnnotationUtils.annotationName(CsvSourceStreamOp.class), params);
	}

	public CsvSourceStreamOp(String filePath, TableSchema schema) {
		this(new Params()
			.set(FILE_PATH, filePath)
			.set(SCHEMA_STR, TableUtil.schema2SchemaStr(schema))
		);
	}

	public CsvSourceStreamOp(String filePath, String schemaStr) {
		this(new Params()
			.set(FILE_PATH, filePath)
			.set(SCHEMA_STR, schemaStr)
		);
	}

	public CsvSourceStreamOp(String filePath, String schemaStr, Params params) {
		this(new Params()
			.set(FILE_PATH, filePath)
			.set(SCHEMA_STR, schemaStr)
			.merge(params)
		);
	}

	public CsvSourceStreamOp(
		String filePath, String[] colNames, TypeInformation <?>[] colTypes, String fieldDelim, String rowDelim) {
		this(new Params()
			.set(FILE_PATH, filePath)
			.set(SCHEMA_STR, TableUtil.schema2SchemaStr(new TableSchema(colNames, colTypes)))
			.set(FIELD_DELIMITER, fieldDelim)
			.set(ROW_DELIMITER, rowDelim)
		);
	}

	@Override
	public Table initializeDataSource() {

		String filePath = getFilePath();
		String schemaStr = getSchemaStr();
		String fieldDelim = getFieldDelimiter();
		String rowDelim = getRowDelimiter();

		fieldDelim = CsvUtil.unEscape(fieldDelim);
		rowDelim = CsvUtil.unEscape(rowDelim);

		String[] colNames = CsvUtil.getColNames(schemaStr);
		TypeInformation[] colTypes = CsvUtil.getColTypes(schemaStr);

		boolean ignoreFirstLine = getIgnoreFirstLine();

		this.init(TableUtil.getTempTableName(), filePath, colNames, colTypes, fieldDelim, rowDelim, ignoreFirstLine);
		return this.table;
	}

	public void init(
		String tableName, String filePath, String[] colNames, TypeInformation <?>[] colTypes, String fieldDelim,
		String rowDelim, boolean ignoreFirstLine) {
		String protocol = null;
		URL url = null;

		try {
			url = new URL(filePath);
			protocol = url.getProtocol();
		} catch (Exception e) {
			protocol = "";
		}

		if (protocol.equalsIgnoreCase("http") || protocol.equalsIgnoreCase("https")) {
			DataStream <Row> data = MLSession.getStreamExecutionEnvironment()
				.createInput(new HttpRowCsvInputFormat(filePath, colTypes, fieldDelim, rowDelim, ignoreFirstLine),
					new RowTypeInfo(colTypes, colNames))
				.name("http_csv_source");
			setTable(data);
		} else {
			CsvTableSource csvTableSource = new CsvTableSource(
				filePath, colNames, colTypes, fieldDelim, rowDelim,
				null, // quoteCharacter
				ignoreFirstLine, // ignoreFirstLine
				null, //"%",    // ignoreComments
				false); // lenient
			MLSession.getStreamTableEnvironment().registerTableSource(tableName, csvTableSource);
			this.table = MLSession.getStreamTableEnvironment().scan(tableName);
		}

		// Fix bug: This is a workaround to deal with the bug of SQL planner.
		// In this bug, CsvSource linked to UDTF  will throw an exception:
		// "Cannot generate a valid execution plan for the given query".
		DataStream <Row> out = RowTypeDataStream.fromTable(this.table)
			.map(new MapFunction <Row, Row>() {
				@Override
				public Row map(Row value) throws Exception {
					return value;
				}
			});

		this.table = RowTypeDataStream.toTable(out, colNames, colTypes);
	}
}
