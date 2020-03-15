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

package org.apache.flink.orc;

import org.apache.flink.core.fs.Path;
import org.apache.flink.orc.shim.OrcShim;
import org.apache.flink.types.Row;

import org.apache.hadoop.conf.Configuration;
import org.apache.orc.TypeDescription;

import java.io.IOException;
import java.util.List;

/**
 * {@link OrcSplitReader} to read ORC files into {@link Row}.
 */
public class OrcRowSplitReader extends OrcSplitReader<Row> {

	private final TypeDescription schema;
	private final int[] selectedFields;
	// the vector of rows that is read in a batch
	private final Row[] rows;

	public OrcRowSplitReader(
			Configuration conf,
			TypeDescription schema,
			int[] selectedFields,
			List<Predicate> conjunctPredicates,
			int batchSize,
			Path path,
			long splitStart,
			long splitLength) throws IOException {
		super(OrcShim.defaultShim(), conf, schema, selectedFields, conjunctPredicates, batchSize, path, splitStart, splitLength);
		this.schema = schema;
		this.selectedFields = selectedFields;
		// create and initialize the row batch
		this.rows = new Row[batchSize];
		for (int i = 0; i < batchSize; i++) {
			rows[i] = new Row(selectedFields.length);
		}
	}

	@Override
	protected int fillRows() {
		return OrcBatchReader.fillRows(rows, schema, rowBatch, selectedFields);
	}

	@Override
	public Row nextRecord(Row reuse) {
		// return the next row
		return rows[this.nextRow++];
	}
}
