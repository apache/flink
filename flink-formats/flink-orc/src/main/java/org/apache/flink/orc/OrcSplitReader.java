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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.fs.Path;
import org.apache.flink.orc.shim.OrcShim;
import org.apache.flink.orc.vector.OrcVectorizedBatchWrapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * Orc split reader to read record from orc file. The reader is only responsible for reading the data
 * of a single split.
 */
public abstract class OrcSplitReader<T, BATCH> implements Closeable {

	private final OrcShim<BATCH> shim;

	// the ORC reader
	private RecordReader orcRowsReader;

	// the vectorized row data to be read in a batch
	protected final OrcVectorizedBatchWrapper<BATCH> rowBatchWrapper;

	// the number of rows in the current batch
	private int rowsInBatch;
	// the index of the next row to return
	protected int nextRow;

	public OrcSplitReader(
			OrcShim<BATCH> shim,
			Configuration conf,
			TypeDescription schema,
			int[] selectedFields,
			List<OrcFilters.Predicate> conjunctPredicates,
			int batchSize,
			Path path,
			long splitStart,
			long splitLength) throws IOException {
		this.shim = shim;
		this.orcRowsReader = shim.createRecordReader(
				conf, schema, selectedFields, conjunctPredicates, path, splitStart, splitLength);

		// create row batch
		this.rowBatchWrapper = shim.createBatchWrapper(schema, batchSize);
		rowsInBatch = 0;
		nextRow = 0;
	}

	/**
	 * Seek to a particular row number.
	 */
	public void seekToRow(long rowCount) throws IOException {
		orcRowsReader.seekToRow(rowCount);
	}

	@VisibleForTesting
	public RecordReader getRecordReader() {
		return orcRowsReader;
	}

	/**
	 * Method used to check if the end of the input is reached.
	 *
	 * @return True if the end is reached, otherwise false.
	 * @throws IOException Thrown, if an I/O error occurred.
	 */
	public boolean reachedEnd() throws IOException {
		return !ensureBatch();
	}

	/**
	 * Fills an ORC batch into an array of Row.
	 *
	 * @return The number of rows that were filled.
	 */
	protected abstract int fillRows();

	/**
	 * Reads the next record from the input.
	 *
	 * @param reuse Object that may be reused.
	 * @return Read record.
	 *
	 * @throws IOException Thrown, if an I/O error occurred.
	 */
	public abstract T nextRecord(T reuse) throws IOException;

	/**
	 * Checks if there is at least one row left in the batch to return.
	 * If no more row are available, it reads another batch of rows.
	 *
	 * @return Returns true if there is one more row to return, false otherwise.
	 * @throws IOException throw if an exception happens while reading a batch.
	 */
	private boolean ensureBatch() throws IOException {

		if (nextRow >= rowsInBatch) {
			// Try to read the next batch if rows from the ORC file.
			boolean moreRows = shim.nextBatch(orcRowsReader, rowBatchWrapper.getBatch());

			if (moreRows) {
				// No more rows available in the Rows array.
				nextRow = 0;
				// Load the data into the Rows array.
				rowsInBatch = fillRows();
			}
			return moreRows;
		}
		// there is at least one Row left in the Rows array.
		return true;
	}

	@Override
	public void close() throws IOException {
		if (orcRowsReader != null) {
			this.orcRowsReader.close();
		}
		this.orcRowsReader = null;
	}
}
