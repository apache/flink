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

package org.apache.flink.ml.batchoperator.source;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.ml.common.MLSession;
import org.apache.flink.ml.common.utils.RowTypeDataSet;
import org.apache.flink.ml.common.utils.TableUtil;
import org.apache.flink.ml.io.AnnotationUtils;
import org.apache.flink.ml.io.IONameAnnotation;
import org.apache.flink.ml.io.http.HttpRowCsvInputFormat;
import org.apache.flink.ml.io.utils.CsvUtil;
import org.apache.flink.ml.params.Params;
import org.apache.flink.ml.params.io.CsvSourceParams;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.types.Row;

import java.net.URL;

/**
 * Batch source for CSV data.
 */
@IONameAnnotation(name = "csv")
public final class CsvSourceBatchOp extends BaseSourceBatchOp <CsvSourceBatchOp>
	implements CsvSourceParams <CsvSourceBatchOp> {

	public CsvSourceBatchOp() {
		this(new Params());
	}

	public CsvSourceBatchOp(Params params) {
		super(AnnotationUtils.annotationName(CsvSourceBatchOp.class), params);
	}

	public CsvSourceBatchOp(String filePath, TableSchema schema) {
		this(new Params()
			.set(FILE_PATH, filePath)
			.set(SCHEMA_STR, TableUtil.schema2SchemaStr(schema))
		);
	}

	public CsvSourceBatchOp(String filePath, String schemaStr) {
		this(new Params()
			.set(FILE_PATH, filePath)
			.set(SCHEMA_STR, schemaStr)
		);
	}

	public CsvSourceBatchOp(String filePath, String schemaStr, Params params) {
		this(new Params()
			.set(FILE_PATH, filePath)
			.set(SCHEMA_STR, schemaStr)
			.merge(params)
		);
	}

	public CsvSourceBatchOp(String filePath, String[] colNames, TypeInformation <?>[] colTypes,
							String fieldDelim, String rowDelim) {
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

	private void init(
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
			DataSet <Row> data = MLSession.getExecutionEnvironment()
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
			MLSession.getBatchTableEnvironment().registerTableSource(tableName, csvTableSource);
			this.table = MLSession.getBatchTableEnvironment().scan(tableName);
		}

		// Fix bug: This is a workaround to deal with the bug of SQL planner.
		// In this bug, CsvSource linked to UDTF  will throw an exception:
		// "Cannot generate a valid execution plan for the given query".
		DataSet <Row> out = RowTypeDataSet.fromTable(this.table)
			.map(new MapFunction <Row, Row>() {
				@Override
				public Row map(Row value) throws Exception {
					return value;
				}
			});

		this.table = RowTypeDataSet.toTable(out, colNames, colTypes);
	}
}
