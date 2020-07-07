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

package org.apache.flink.formats.hadoop.bulk;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.BulkWriter;

import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.Serializable;

/**
 * Specialized {@link BulkWriter} which is expected to write to specified
 * {@link Path}.
 */
@Internal
public interface HadoopPathBasedBulkWriter<T> extends BulkWriter<T> {

	/**
	 * Gets the size written by the current writer.
	 *
	 * @return The size written by the current writer.
	 */
	long getSize() throws IOException;

	/**
	 * Disposes the writer on failures. Unlike output-stream-based writers which
	 * could handled uniformly by closing the underlying output stream, the path-
	 * based writers need to be disposed explicitly.
	 */
	void dispose();

	// ------------------------------------------------------------------------

	/**
	 * A factory that creates a {@link HadoopPathBasedBulkWriter}.
	 *
	 * @param <T> The type of record to write.
	 */
	@FunctionalInterface
	interface Factory<T> extends Serializable {

		/**
		 * Creates a path-based writer that writes to the <tt>inProgressPath</tt> first
		 * and commits to <tt>targetPath</tt> finally.
		 *
		 * @param targetFilePath The final path to commit to.
		 * @param inProgressFilePath The intermediate path to write to before committing.
		 * @return The created writer.
		 */
		HadoopPathBasedBulkWriter<T> create(Path targetFilePath, Path inProgressFilePath) throws IOException;

	}
}
