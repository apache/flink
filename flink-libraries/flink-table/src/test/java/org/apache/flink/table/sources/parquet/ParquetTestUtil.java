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

package org.apache.flink.table.sources.parquet;

import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.table.api.types.InternalType;
import org.apache.flink.table.api.types.RowType;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.GenericRow;
import org.apache.flink.table.dataformat.vector.VectorizedColumnBatch;
import org.apache.flink.table.runtime.conversion.DataStructureConverters;
import org.apache.flink.table.sinks.parquet.RowParquetOutputFormat;
import org.apache.flink.types.Row;

import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import scala.Function1;

import static junit.framework.TestCase.assertEquals;

/**
 * A parquet helper class to write and read.
 */
public class ParquetTestUtil {

	public static void writeParquet(
		String path,
		InternalType[] fieldTypes,
		String[] fieldNames,
		int blockSize,
		boolean enableDictionary,
		CompressionCodecName compression,
		Iterator<GenericRow> rowIterator) throws IOException {
		File file = new File(path);
		if (file.exists()) {
			deleteDir(file);
		}
		RowParquetOutputFormat outFormat = new RowParquetOutputFormat(
			path,
			fieldTypes,
			fieldNames, compression, blockSize, enableDictionary);
		outFormat.configure(null);
		outFormat.open(1, 1);

		while (rowIterator.hasNext()) {
			outFormat.writeRecord(rowIterator.next());
		}
		outFormat.close();
	}

	public static void readParquet(ParquetInputFormat inputFormat, int spilt,
			ConvertRecord convertRecord) throws IOException {
		inputFormat.setNestedFileEnumeration(true);
		FileInputSplit[] splits = inputFormat.createInputSplits(spilt);
		for (int i = 0; i < spilt; i++) {
			inputFormat.open(splits[i]);
			while (!inputFormat.reachedEnd()) {
				Object ret = inputFormat.nextRecord(null);
				convertRecord.convert(ret);
			}
			inputFormat.close();

		}
	}

	public static void checkWriteParquet(
		String path, InternalType[] fieldTypes, String[] fieldNames, int blockSize,
		boolean enableDictionary, CompressionCodecName compression, int split,
		Iterator<GenericRow> rowIterator, List<GenericRow> expertRows) throws IOException {

		List<Row> actualRows = new ArrayList<>();

		ParquetTestUtil.writeParquet(
			path,
			fieldTypes,
			fieldNames,
			blockSize,
			enableDictionary,
			compression,
			rowIterator);

		//read
		VectorizedBatchParquetInputFormat inputFormat = new VectorizedBatchParquetInputFormat(
			new org.apache.flink.core.fs.Path(path),
			fieldTypes,
			fieldNames);
		ParquetTestUtil.readParquet(
			inputFormat,
			split,
			new ParquetTestUtil.ConvertVectorBatch2Row(actualRows));

		Function1<BaseRow, Row> converter = (Function1) DataStructureConverters
				.createToExternalConverter(new RowType(fieldTypes));
		//verify
		assertEquals(expertRows.stream().map(converter :: apply).collect(Collectors.toList()),
				actualRows);
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
		private int generatorSize;

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
}
