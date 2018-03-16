/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.graph.asm.result;

import org.apache.flink.graph.asm.translate.TranslateFunction;
import org.apache.flink.util.Collector;

/**
 * Base class for algorithm results for a single vertex.
 *
 * @param <K> graph ID type
 */
public abstract class UnaryResultBase<K>
extends ResultBase
implements UnaryResult<K>, TranslatableResult<K> {

	private K vertexId0;

	@Override
	public K getVertexId0() {
		return vertexId0;
	}

	@Override
	public void setVertexId0(K vertexId0) {
		this.vertexId0 = vertexId0;
	}

	@Override
	public <T> TranslatableResult<T> translate(TranslateFunction<K, T> translator, TranslatableResult<T> reuse, Collector<TranslatableResult<T>> out)
			throws Exception {
		if (reuse == null) {
			reuse = new BasicUnaryResult<>();
		}

		K vertexId0 = this.getVertexId0();

		UnaryResult<T> translatable = (UnaryResult<T>) reuse;
		UnaryResult<T> translated = (UnaryResult<T>) this;

		translated.setVertexId0(translator.translate(this.getVertexId0(), translatable.getVertexId0()));

		out.collect((TranslatableResult<T>) translated);

		this.setVertexId0(vertexId0);

		return reuse;
	}

	/**
	 * Simple override of {@code UnaryResultBase}. This holds no additional
	 * values but is used by {@link UnaryResultBase#translate} as the reuse
	 * object for translating vertex IDs.
	 *
	 * @param <U> result ID type
	 */
	private static class BasicUnaryResult<U>
	extends UnaryResultBase<U> {
		@Override
		public String toString() {
			return "(" + getVertexId0() + ")";
		}
	}
}
