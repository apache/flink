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

package org.apache.flink.graph.asm.result;

import org.apache.flink.graph.asm.translate.TranslateFunction;
import org.apache.flink.util.Collector;

/**
 * A transformable result can transform its ID type.
 *
 * @param <K> graph ID type
 */
public interface TranslatableResult<K> {

	/**
	 * Output the result after transforming the vertex ID type.
	 *
	 * @param translator translates type {@code K} to type {@code T}
	 * @param reuse reusable value
	 * @param out output collector
	 * @return reusable result
	 * @throws Exception on error
	 *
	 * @param <T> ID output type
	 */
	<T> TranslatableResult<T> translate(TranslateFunction<K, T> translator, TranslatableResult<T> reuse, Collector<TranslatableResult<T>> out) throws Exception;
}
