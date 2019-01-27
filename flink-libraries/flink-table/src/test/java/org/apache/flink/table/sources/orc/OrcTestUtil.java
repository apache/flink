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

package org.apache.flink.table.sources.orc;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.table.api.types.BooleanType;
import org.apache.flink.table.api.types.ByteType;
import org.apache.flink.table.api.types.DataType;
import org.apache.flink.table.api.types.DateType;
import org.apache.flink.table.api.types.DecimalType;
import org.apache.flink.table.api.types.DoubleType;
import org.apache.flink.table.api.types.FloatType;
import org.apache.flink.table.api.types.IntType;
import org.apache.flink.table.api.types.InternalType;
import org.apache.flink.table.api.types.LongType;
import org.apache.flink.table.api.types.ShortType;
import org.apache.flink.table.api.types.StringType;
import org.apache.flink.table.api.types.TimestampType;
import org.apache.flink.table.api.types.TypeConverters;
import org.apache.flink.table.api.types.TypeInfoWrappedDataType;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.ColumnarRow;
import org.apache.flink.table.dataformat.GenericRow;
import org.apache.flink.table.dataformat.vector.VectorizedColumnBatch;
import org.apache.flink.table.runtime.conversion.DataStructureConverters;
import org.apache.flink.table.sinks.orc.RowOrcOutputFormat;
import org.apache.flink.types.Row;
import org.apache.flink.util.TimeConvertUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import scala.Function1;

import static junit.framework.TestCase.assertEquals;

/**
 * A orc helper class to write and read.
 */
public class OrcTestUtil {

	public static void writeOrc(
			InternalType[] fieldTypes,
			String[] fieldNames,
			String path,
			Iterator<GenericRow> rowIterator) throws IOException {
		File file = new File(path);
		if (file.exists()) {
			deleteDir(file);
		}

		RowOrcOutputFormat orc = new RowOrcOutputFormat(fieldTypes, fieldNames, path);
		orc.configure(null);
		orc.open(1, 1);
		while (rowIterator.hasNext()) {
			orc.writeRecord(rowIterator.next());
		}
		orc.close();
	}

	public static void readOrc(
			OrcInputFormat orc,
			int split,
			ConvertRecord convertRecord) throws IOException {
		orc.setNestedFileEnumeration(true);
		FileInputSplit[] splits = orc.createInputSplits(split);
		for (int i = 0; i < splits.length; i++) {
			orc.open(splits[i]);
			while (!orc.reachedEnd()) {
				Object ret = orc.nextRecord(null);
				convertRecord.convert(ret);
			}
			orc.close();
		}
	}

	public static void checkOrcRowBaseFormat(
			InternalType[] fieldTypes,
			String[] fieldNames,
			DataType[] requestFieldTypes,
			String[] requestFieldNames,
			String path,
			int split,
			Iterator<GenericRow> rowIterator,
			OrcInputFormat inputFormat,
			List<GenericRow> expertRows) throws IOException {
		List<Row> actualRows = new ArrayList<>();

		// write to orc file
		writeOrc(fieldTypes, fieldNames, path, rowIterator);

		// read from orc file
		readOrc(inputFormat, split, new Row2Row(actualRows));

		Function1<BaseRow, Row> converter = (Function1) DataStructureConverters
			.createToExternalConverter(
				new TypeInfoWrappedDataType(
					new RowTypeInfo(
						Arrays.stream(requestFieldTypes)
							.map(x -> TypeConverters.createExternalTypeInfoFromDataType(x))
							.toArray(TypeInformation[]::new))));

		// verify
		assertEquals(expertRows.stream().map(converter::apply).collect(Collectors.toList()), actualRows);

		File file = new File(path);
		if (file.exists()) {
			deleteDir(file);
		}
	}

	public static void checkOrcVectorizedBaseFormat(
		InternalType[] fieldTypes,
		String[] fieldNames,
		DataType[] requestFieldTypes,
		String[] requestFieldNames,
		String path,
		int split,
		Iterator<GenericRow> rowIterator,
		OrcInputFormat inputFormat,
		List<GenericRow> expertRows) throws IOException {
		List<Row> actualRows = new ArrayList<>();

		// write to orc file
		writeOrc(fieldTypes, fieldNames, path, rowIterator);

		// read from orc file
		readOrc(inputFormat, split, new ConvertColumnarRow2Row(actualRows, requestFieldTypes));

		Function1<BaseRow, Row> converter = (Function1) DataStructureConverters
			.createToExternalConverter(
				new TypeInfoWrappedDataType(
					new RowTypeInfo(
						Arrays.stream(requestFieldTypes)
							.map(x -> TypeConverters.createExternalTypeInfoFromDataType(x))
							.toArray(TypeInformation[]::new))));

		// verify
		assertEquals(expertRows.stream().map(converter::apply).collect(Collectors.toList()), actualRows);

		File file = new File(path);
		if (file.exists()) {
			deleteDir(file);
		}
	}

	private static boolean deleteDir(File dir) {
		if (dir.isDirectory()) {
			String[] children = dir.list();
			for (int i = 0; i < children.length; i++) {
				boolean success = deleteDir(new File(dir, children[i]));
				if (!success) {
					return false;
				}
			}
		}
		return dir.delete();
	}

	/**
	 * the convert {@link Object} interface.
	 */
	public interface ConvertRecord {
		void convert(Object ret);
	}

	/**
	 * The base GeneratorRow class to generator {@link Row}.
	 */
	public abstract static class GeneratorRow implements Iterator<GenericRow> {
		protected int generatorSize;

		public GeneratorRow(int generatorSize) {
			this.generatorSize = generatorSize;
		}

		@Override
		public boolean hasNext() {
			return generatorSize-- > 0;
		}
	}

	/**
	 * the convert implement from vector batch to rows.
	 */
	public static class ConvertVectorBatch2Row implements ConvertRecord {
		private List<Row> rows;

		public ConvertVectorBatch2Row(List<Row> rows) {
			this.rows = rows;
		}

		@Override
		public void convert(Object ret) {
			VectorizedColumnBatch batch = (VectorizedColumnBatch) ret;
			for (int rowId = 0; rowId < batch.getNumRows(); rowId++) {
				Row row = new Row(batch.getArity());
				for (int colId = 0; colId < batch.getArity(); colId++) {
					row.setField(colId, batch.getObject(rowId, colId));
				}
				rows.add(row);
			}
		}
	}

	/**
	 * the convert implement from row to rows.
	 */
	public static class Row2Row implements ConvertRecord {
		private List<Row> rows;

		public Row2Row(List<Row> rows) {
			this.rows = rows;
		}

		@Override
		public void convert(Object ret) {
			rows.add((Row) ret);
		}
	}

	/**
	 * the convert implement from vector batch to rows.
	 */
	public static class ConvertColumnarRow2Row implements ConvertRecord {
		private List<Row> rows;
		private DataType[] fieldTypes;

		public ConvertColumnarRow2Row(List<Row> rows, DataType[] fieldTypes) {
			this.rows = rows;
			this.fieldTypes = fieldTypes;
		}

		@Override
		public void convert(Object ret) {
			ColumnarRow batch = (ColumnarRow) ret;
			Row row = new Row(batch.getArity());
			for (int colId = 0; colId < batch.getArity(); colId++) {
				if (batch.isNullAt(colId)) {
					row.setField(colId, null);
				} else {
					DataType fieldType = fieldTypes[colId];

					if (fieldType instanceof BooleanType) {
						row.setField(colId, batch.getBoolean(colId));
					} else if (fieldType instanceof ByteType) {
						row.setField(colId, batch.getByte(colId));
					} else if (fieldType instanceof ShortType) {
						row.setField(colId, batch.getShort(colId));
					} else if (fieldType instanceof IntType) {
						row.setField(colId, batch.getInt(colId));
					} else if (fieldType instanceof LongType) {
						row.setField(colId, batch.getLong(colId));
					} else if (fieldType instanceof FloatType) {
						row.setField(colId, batch.getFloat(colId));
					} else if (fieldType instanceof DoubleType) {
						row.setField(colId, batch.getDouble(colId));
					} else if (fieldType instanceof StringType) {
						row.setField(colId, batch.getString(colId));
					} else if (fieldType instanceof DecimalType) {
						int precision = ((DecimalType) fieldType).precision();
						int scale = ((DecimalType) fieldType).scale();
						row.setField(colId, batch.getDecimal(colId, precision, scale).toBigDecimal());
					} else if (fieldType instanceof DateType) {
						row.setField(colId, TimeConvertUtils.internalToDate(batch.getInt(colId)));
					} else if (fieldType instanceof TimestampType) {
						row.setField(colId, TimeConvertUtils.internalToTimestamp(batch.getLong(colId)));
					} else {
						throw new UnsupportedOperationException("Unsupported Data Type: " + fieldType);
					}
				}
			}
			rows.add(row);
		}
	}
}
