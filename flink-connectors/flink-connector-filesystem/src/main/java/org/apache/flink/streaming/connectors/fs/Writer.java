/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.fs;

import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.Serializable;

/**
 * An implementation of {@code Writer} is used in conjunction with a
 * {@link BucketingSink} to perform the actual
 * writing to the bucket files.
 *
 * @param <T> The type of the elements that are being written by the sink.
 */
public interface Writer<T> extends Serializable {

	/**
	 * Initializes the {@code Writer} for a newly opened bucket file.
	 * Any internal per-bucket initialization should be performed here.
	 *
	 * @param fs The {@link org.apache.hadoop.fs.FileSystem} containing the newly opened file.
	 * @param path The {@link org.apache.hadoop.fs.Path} of the newly opened file.
	 */
	void open(FileSystem fs, Path path) throws IOException;

	/**
	 * Flushes out any internally held data, and returns the offset that the file
	 * must be truncated to at recovery.
	 */
	long flush() throws IOException;

	/**
	 * Retrieves the current position, and thus size, of the output file.
	 */
	long getPos() throws IOException;

	/**
	 * Closes the {@code Writer}. If the writer is already closed, no action will be
	 * taken. The call should close all state related to the current output file,
	 * including the output stream opened in {@code open}.
	 */
	void close() throws IOException;

	/**
	 * Writes one element to the bucket file.
	 */
	void write(T element)throws IOException;

	/**
	 * Duplicates the {@code Writer}. This is used to get one {@code Writer} for each
	 * parallel instance of the sink.
	 */
	Writer<T> duplicate();
}
