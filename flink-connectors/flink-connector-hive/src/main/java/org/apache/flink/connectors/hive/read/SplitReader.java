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

package org.apache.flink.connectors.hive.read;

import org.apache.flink.table.data.RowData;

import java.io.Closeable;
import java.io.IOException;

/**
 * Split reader to read record from files. The reader is only responsible for reading the data
 * of a single split.
 */
public interface SplitReader extends Closeable {

	/**
	 * Method used to check if the end of the input is reached.
	 *
	 * @return True if the end is reached, otherwise false.
	 * @throws IOException Thrown, if an I/O error occurred.
	 */
	boolean reachedEnd() throws IOException;

	/**
	 * Reads the next record from the input.
	 *
	 * @param reuse Object that may be reused.
	 * @return Read record.
	 *
	 * @throws IOException Thrown, if an I/O error occurred.
	 */
	RowData nextRecord(RowData reuse) throws IOException;

	/**
	 * Seek to a particular row number.
	 */
	default void seekToRow(long rowCount, RowData reuse) throws IOException {
		for (int i = 0; i < rowCount; i++) {
			boolean end = reachedEnd();
			if (end) {
				throw new RuntimeException("Seek too many rows.");
			}
			nextRecord(reuse);
		}
	}
}
