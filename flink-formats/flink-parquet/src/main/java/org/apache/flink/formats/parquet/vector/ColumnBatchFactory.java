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

package org.apache.flink.formats.parquet.vector;

import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.table.data.vector.ColumnVector;
import org.apache.flink.table.data.vector.VectorizedColumnBatch;

import java.io.Serializable;

/**
 * Interface to create {@link VectorizedColumnBatch}.
 */
@FunctionalInterface
public interface ColumnBatchFactory<SplitT extends FileSourceSplit> extends Serializable {

	VectorizedColumnBatch create(SplitT split, ColumnVector[] vectors);

	static <SplitT extends FileSourceSplit> ColumnBatchFactory<SplitT> withoutExtraFields() {
		return (ColumnBatchFactory<SplitT>) (split, vectors) -> new VectorizedColumnBatch(vectors);
	}
}
