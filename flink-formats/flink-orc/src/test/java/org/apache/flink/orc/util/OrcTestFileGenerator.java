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

package org.apache.flink.orc.util;

import org.apache.flink.orc.OrcRowInputFormatTest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.MapColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * A generator for ORC test files.
 */
public class OrcTestFileGenerator {

	public static void main(String[] args) throws IOException {
		writeCompositeTypesWithNullsFile(args[0]);
//		writeCompositeTypesWithRepeatingFile(args[0]);
	}

	/**
	 * Writes an ORC file with nested composite types and null values on different levels.
	 * Generates {@link OrcRowInputFormatTest#TEST_FILE_COMPOSITES_NULLS}.
	 */
	private static void writeCompositeTypesWithNullsFile(String path) throws IOException {

		Path filePath = new Path(path);
		Configuration conf = new Configuration();

		TypeDescription schema =
			TypeDescription.fromString(
				"struct<" +
					"int1:int," +
					"record1:struct<f1:int,f2:string>," +
					"list1:array<array<array<struct<f1:string,f2:string>>>>," +
					"list2:array<map<string,int>>" +
				">");

		Writer writer =
			OrcFile.createWriter(filePath,
				OrcFile.writerOptions(conf).setSchema(schema));

		VectorizedRowBatch batch = schema.createRowBatch();
		LongColumnVector int1 = (LongColumnVector) batch.cols[0];

		StructColumnVector record1 = (StructColumnVector) batch.cols[1];
		LongColumnVector record1F1 = (LongColumnVector) record1.fields[0];
		BytesColumnVector record1F2 = (BytesColumnVector) record1.fields[1];

		ListColumnVector list1 = (ListColumnVector) batch.cols[2];
		ListColumnVector nestedList = (ListColumnVector) list1.child;
		ListColumnVector nestedList2 = (ListColumnVector) nestedList.child;
		StructColumnVector listEntries = (StructColumnVector) nestedList2.child;
		BytesColumnVector entryField1 = (BytesColumnVector) listEntries.fields[0];
		BytesColumnVector entryField2 = (BytesColumnVector) listEntries.fields[1];

		ListColumnVector list2 = (ListColumnVector) batch.cols[3];
		MapColumnVector map1 = (MapColumnVector) list2.child;
		BytesColumnVector keys = (BytesColumnVector) map1.keys;
		LongColumnVector vals = (LongColumnVector) map1.values;

		final int list1Size = 4;
		final int nestedListSize = 3;
		final int nestedList2Size = 2;
		final int list2Size = 3;
		final int mapSize = 3;

		final int batchSize = batch.getMaxSize();

		// Ensure the vectors have sufficient capacity
		nestedList.ensureSize(batchSize * list1Size, false);
		nestedList2.ensureSize(batchSize * list1Size * nestedListSize, false);
		listEntries.ensureSize(batchSize * list1Size * nestedListSize * nestedList2Size, false);
		map1.ensureSize(batchSize * list2Size, false);
		keys.ensureSize(batchSize * list2Size * mapSize, false);
		vals.ensureSize(batchSize * list2Size * mapSize, false);

		// add 2500 rows to file
		for (int r = 0; r < 2500; ++r) {
			int row = batch.size++;

			// mark nullable fields
			list1.noNulls = false;
			nestedList.noNulls = false;
			listEntries.noNulls = false;
			entryField1.noNulls = false;
			record1.noNulls = false;
			record1F2.noNulls = false;
			list2.noNulls = false;
			map1.noNulls = false;
			keys.noNulls = false;
			vals.noNulls = false;

			// first field: int1
			int1.vector[row] = r;

			// second field: struct
			if (row % 2 != 0) {
				// in every second row, the struct is null
				record1F1.vector[row] = row;
				if (row % 5 != 0) {
					// in every fifth row, the second field of the struct is null
					record1F2.setVal(row, ("f2-" + row).getBytes(StandardCharsets.UTF_8));
				} else {
					record1F2.isNull[row] = true;
				}
			} else {
				record1.isNull[row] = true;
			}

			// third field: deeply nested list
			if (row % 3 != 0) {
				// in every third row, the nested list is null
				list1.offsets[row] = list1.childCount;
				list1.lengths[row] = list1Size;
				list1.childCount += list1Size;

				for (int i = 0; i < list1Size; i++) {

					int listOffset = (int) list1.offsets[row] + i;
					if (i != 2) {
						// second nested list is always null
						nestedList.offsets[listOffset] = nestedList.childCount;
						nestedList.lengths[listOffset] = nestedListSize;
						nestedList.childCount += nestedListSize;

						for (int j = 0; j < nestedListSize; j++) {
							int nestedOffset = (int) nestedList.offsets[listOffset] + j;
							nestedList2.offsets[nestedOffset] = nestedList2.childCount;
							nestedList2.lengths[nestedOffset] = nestedList2Size;
							nestedList2.childCount += nestedList2Size;

							for (int k = 0; k < nestedList2Size; k++) {
								int nestedOffset2 = (int) nestedList2.offsets[nestedOffset] + k;
								// list entries
								if (k != 1) {
									// second struct is always null
									if (k != 0) {
										// first struct field in first struct is always null
										entryField1.setVal(nestedOffset2, ("f1-" + k).getBytes(StandardCharsets.UTF_8));
									} else {
										entryField1.isNull[nestedOffset2] = true;
									}
									entryField2.setVal(nestedOffset2, ("f2-" + k).getBytes(StandardCharsets.UTF_8));
								} else {
									listEntries.isNull[nestedOffset2] = true;
								}
							}
						}
					} else {
						nestedList.isNull[listOffset] = true;
					}
				}
			} else {
				list1.isNull[row] = true;
			}

			// forth field: map in list
			if (row % 3 != 0) {
				// in every third row, the map list is null
				list2.offsets[row] = list2.childCount;
				list2.lengths[row] = list2Size;
				list2.childCount += list2Size;

				for (int i = 0; i < list2Size; i++) {
					int mapOffset = (int) list2.offsets[row] + i;

					if (i != 2) {
						// second map list entry is always null
						map1.offsets[mapOffset] = map1.childCount;
						map1.lengths[mapOffset] = mapSize;
						map1.childCount += mapSize;

						for (int j = 0; j < mapSize; j++) {
							int mapEntryOffset = (int) map1.offsets[mapOffset] + j;

							if (j != 1) {
								// key in second map entry is always null
								keys.setVal(mapEntryOffset, ("key-" + row + "-" + j).getBytes(StandardCharsets.UTF_8));
							} else {
								keys.isNull[mapEntryOffset] = true;
							}
							if (j != 2) {
								// value in third map entry is always null
								vals.vector[mapEntryOffset] = row + i + j;
							} else {
								vals.isNull[mapEntryOffset] = true;
							}
						}
					} else {
						map1.isNull[mapOffset] = true;
					}
				}
			} else {
				list2.isNull[row] = true;
			}

			if (row == batchSize - 1) {
				writer.addRowBatch(batch);
				batch.reset();
			}
		}
		if (batch.size != 0) {
			writer.addRowBatch(batch);
			batch.reset();
		}
		writer.close();
	}

	/**
	 * Writes an ORC file with nested composite types and repeated values.
	 * Generates {@link OrcRowInputFormatTest#TEST_FILE_REPEATING}.
	 */
	private static void writeCompositeTypesWithRepeatingFile(String path) throws IOException {

		Path filePath = new Path(path);
		Configuration conf = new Configuration();

		TypeDescription schema =
			TypeDescription.fromString(
				"struct<" +
					"int1:int," +
					"int2:int," +
					"int3:int," +
					"record1:struct<f1:int,f2:string>," +
					"record2:struct<f1:int,f2:string>," +
					"list1:array<int>," +
					"list2:array<int>," +
					"list3:array<int>," +
					"map1:map<int,string>," +
					"map2:map<int,string>" +
				">");

		Writer writer =
			OrcFile.createWriter(filePath,
				OrcFile.writerOptions(conf).setSchema(schema));

		VectorizedRowBatch batch = schema.createRowBatch();

		LongColumnVector int1 = (LongColumnVector) batch.cols[0];
		LongColumnVector int2 = (LongColumnVector) batch.cols[1];
		LongColumnVector int3 = (LongColumnVector) batch.cols[2];

		StructColumnVector record1 = (StructColumnVector) batch.cols[3];
		LongColumnVector record1F1 = (LongColumnVector) record1.fields[0];
		BytesColumnVector record1F2 = (BytesColumnVector) record1.fields[1];
		StructColumnVector record2 = (StructColumnVector) batch.cols[4];

		ListColumnVector list1 = (ListColumnVector) batch.cols[5];
		LongColumnVector list1int = (LongColumnVector) list1.child;
		ListColumnVector list2 = (ListColumnVector) batch.cols[6];
		LongColumnVector list2int = (LongColumnVector) list2.child;
		ListColumnVector list3 = (ListColumnVector) batch.cols[7];

		MapColumnVector map1 = (MapColumnVector) batch.cols[8];
		LongColumnVector map1keys = (LongColumnVector) map1.keys;
		BytesColumnVector map1vals = (BytesColumnVector) map1.values;
		MapColumnVector map2 = (MapColumnVector) batch.cols[9];

		final int listSize = 3;
		final int mapSize = 2;

		final int batchSize = batch.getMaxSize();

		// Ensure the vectors have sufficient capacity
		list1int.ensureSize(batchSize * listSize, false);
		list2int.ensureSize(batchSize * listSize, false);
		map1keys.ensureSize(batchSize * mapSize, false);
		map1vals.ensureSize(batchSize * mapSize, false);

		// int1: all values are 42
		int1.noNulls = true;
		int1.setRepeating(true);
		int1.vector[0] = 42;

		// int2: all values are null
		int2.noNulls = false;
		int2.setRepeating(true);
		int2.isNull[0] = true;

		// int3: all values are 99
		int3.noNulls = false;
		int3.setRepeating(true);
		int3.isNull[0] = false;
		int3.vector[0] = 99;

		// record1: all records are [23, "Hello"]
		record1.noNulls = true;
		record1.setRepeating(true);
		for (int i = 0; i < batchSize; i++) {
			record1F1.vector[i] = i + 23;
		}
		record1F2.noNulls = false;
		record1F2.isNull[0] = true;

		// record2: all records are null
		record2.noNulls = false;
		record2.setRepeating(true);
		record2.isNull[0] = true;

		// list1: all lists are [1, 2, 3]
		list1.noNulls = true;
		list1.setRepeating(true);
		list1.lengths[0] = listSize;
		list1.offsets[0] = 1;
		for (int i = 0; i < batchSize * listSize; i++) {
			list1int.vector[i] = i;
		}

		// list2: all lists are [7, 7, 7]
		list2.noNulls = true;
		list2.setRepeating(true);
		list2.lengths[0] = listSize;
		list2.offsets[0] = 0;
		list2int.setRepeating(true);
		list2int.vector[0] = 7;

		// list3: all lists are null
		list3.noNulls = false;
		list3.setRepeating(true);
		list3.isNull[0] = true;

		// map1: all maps are [2 -> "HELLO", 4 -> "HELLO"]
		map1.noNulls = true;
		map1.setRepeating(true);
		map1.lengths[0] = mapSize;
		map1.offsets[0] = 1;
		for (int i = 0; i < batchSize * mapSize; i++) {
			map1keys.vector[i] = i * 2;
		}
		map1vals.setRepeating(true);
		map1vals.setVal(0, "Hello".getBytes(StandardCharsets.UTF_8));

		// map2: all maps are null
		map2.noNulls = false;
		map2.setRepeating(true);
		map2.isNull[0] = true;

		batch.size = 256;

		writer.addRowBatch(batch);
		batch.reset();
		writer.close();
	}

}
