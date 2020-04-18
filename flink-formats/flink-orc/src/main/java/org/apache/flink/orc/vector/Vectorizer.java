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

package org.apache.flink.orc.vector;

import org.apache.flink.annotation.PublicEvolving;

import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.TypeDescription;

import java.io.IOException;
import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This class provides an abstracted set of methods to handle the lifecycle of {@link VectorizedRowBatch}.
 *
 * <p>Developers have to extend this class and override the vectorize() method with the logic
 * to transform the element to a {@link VectorizedRowBatch}.
 *
 * @param <T> The type of the element
 */
@PublicEvolving
public abstract class Vectorizer<T> implements Serializable {

	protected transient VectorizedRowBatch rowBatch;

	private final TypeDescription schema;

	public Vectorizer(final String schema) {
		checkNotNull(schema);
		this.schema = TypeDescription.fromString(schema);
		this.rowBatch = this.schema.createRowBatch();
	}

	public void initBatch() {
		this.rowBatch = this.schema.createRowBatch();
	}

	/**
	 * Provides the ORC schema.
	 *
	 * @return the ORC schema
	 */
	public TypeDescription getSchema() {
		return this.schema;
	}

	/**
	 * Provides the VectorizedRowBatch containing the
	 * ColumnVectors of the input elements.
	 *
	 * @return vectorized row batch
	 */
	public VectorizedRowBatch getRowBatch() {
		return this.rowBatch;
	}

	/**
	 * Calls reset on the VectorizedRowBatch instance.
	 */
	public void reset() {
		if (this.rowBatch != null) {
			this.rowBatch.reset();
		}
	}

	/**
	 * Transforms the provided element to ColumnVectors and
	 * sets them in the exposed VectorizedRowBatch.
	 *
	 * @param element The input element
	 * @throws IOException if there is an error while transforming the input.
	 */
	public abstract void vectorize(T element) throws IOException;

}
