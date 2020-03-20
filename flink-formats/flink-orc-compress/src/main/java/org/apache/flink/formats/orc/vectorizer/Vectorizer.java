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

package org.apache.flink.formats.orc.vectorizer;

import org.apache.flink.annotation.PublicEvolving;

import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;

import java.io.IOException;
import java.io.Serializable;

/**
 * Implementors of this interface provide the logic to transform their data to {@link VectorizedRowBatch}.
 *
 * @param <T> The type of the element
 */
@PublicEvolving
public interface Vectorizer<T> extends Serializable {

	/**
	 * Creates a VectorizedRowBatch containing an array of ColumnVectors
	 * from the provided element.
	 *
	 * @param element The input element
	 * @return The VectorizedRowBatch containing the ColumnVectors of the input element
	 * @throws IOException if there is an error while transforming the input.
	 */
	VectorizedRowBatch vectorize(T element) throws IOException;

}
