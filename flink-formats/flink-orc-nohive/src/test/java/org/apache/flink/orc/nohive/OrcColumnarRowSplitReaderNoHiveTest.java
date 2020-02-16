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

package org.apache.flink.orc.nohive;

import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.orc.OrcColumnarRowSplitReader;
import org.apache.flink.orc.OrcColumnarRowSplitReaderTest;
import org.apache.flink.table.types.DataType;

import org.apache.hadoop.conf.Configuration;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.orc.storage.ql.exec.vector.DoubleColumnVector;
import org.apache.orc.storage.ql.exec.vector.LongColumnVector;
import org.apache.orc.storage.ql.exec.vector.TimestampColumnVector;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Map;
import java.util.stream.IntStream;

/**
 * Test for {@link OrcColumnarRowSplitReader}.
 */
public class OrcColumnarRowSplitReaderNoHiveTest extends OrcColumnarRowSplitReaderTest {

	@Override
	protected void prepareReadFileWithTypes(String file, int rowSize) throws IOException {
		// NOTE: orc has field name information, so name should be same as orc
		TypeDescription schema =
				TypeDescription.fromString(
						"struct<" +
								"f0:float," +
								"f1:double," +
								"f2:timestamp," +
								"f3:tinyint," +
								"f4:smallint" +
								">");

		org.apache.hadoop.fs.Path filePath = new org.apache.hadoop.fs.Path(file);
		Configuration conf = new Configuration();

		Writer writer =
				OrcFile.createWriter(filePath,
						OrcFile.writerOptions(conf).setSchema(schema));

		VectorizedRowBatch batch = schema.createRowBatch(rowSize);
		DoubleColumnVector col0 = (DoubleColumnVector) batch.cols[0];
		DoubleColumnVector col1 = (DoubleColumnVector) batch.cols[1];
		TimestampColumnVector col2 = (TimestampColumnVector) batch.cols[2];
		LongColumnVector col3 = (LongColumnVector) batch.cols[3];
		LongColumnVector col4 = (LongColumnVector) batch.cols[4];

		col0.noNulls = false;
		col1.noNulls = false;
		col2.noNulls = false;
		col3.noNulls = false;
		col4.noNulls = false;
		for (int i = 0; i < rowSize - 1; i++) {
			col0.vector[i] = i;
			col1.vector[i] = i;

			Timestamp timestamp = toTimestamp(i);
			col2.time[i] = timestamp.getTime();
			col2.nanos[i] = timestamp.getNanos();

			col3.vector[i] = i;
			col4.vector[i] = i;
		}

		col0.isNull[rowSize - 1] = true;
		col1.isNull[rowSize - 1] = true;
		col2.isNull[rowSize - 1] = true;
		col3.isNull[rowSize - 1] = true;
		col4.isNull[rowSize - 1] = true;

		batch.size = rowSize;
		writer.addRowBatch(batch);
		batch.reset();
		writer.close();
	}

	@Override
	protected OrcColumnarRowSplitReader createReader(
			int[] selectedFields,
			DataType[] fullTypes,
			Map<String, Object> partitionSpec,
			FileInputSplit split) throws IOException {
		return OrcNoHiveSplitReaderUtil.genPartColumnarRowReader(
				new Configuration(),
				IntStream.range(0, fullTypes.length)
						.mapToObj(i -> "f" + i).toArray(String[]::new),
				fullTypes,
				partitionSpec,
				selectedFields,
				new ArrayList<>(),
				BATCH_SIZE,
				split.getPath(),
				split.getStart(),
				split.getLength());
	}
}
