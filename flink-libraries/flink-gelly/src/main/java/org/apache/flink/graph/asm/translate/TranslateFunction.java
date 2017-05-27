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

package org.apache.flink.graph.asm.translate;

import org.apache.flink.api.common.functions.Function;

import java.io.Serializable;

/**
 * Base interface for Translate functions. Translate functions take elements and transform them,
 * element wise. A Translate function always produces a single result element for each input element.
 * Typical applications are transcribing between data types or manipulating element values.
 *
 * <p>Translate functions are used within the Graph API and by translating GraphAlgorithms.
 *
 * @param <T> Type of the input elements.
 * @param <O> Type of the returned elements.
 */
public interface TranslateFunction<T, O> extends Function, Serializable {

	/**
	 * The translating method. Takes an element from the input data set and transforms
	 * it into exactly one element.
	 *
	 * @param value input value.
	 * @param reuse value which may be reused for output; if reuse is null then a new output object
	 *              must be instantiated and returned
	 * @return the transformed value
	 *
	 * @throws Exception This method may throw exceptions. Throwing an exception will cause the operation
	 *                   to fail and may trigger recovery.
	 */
	O translate(T value, O reuse) throws Exception;
}
