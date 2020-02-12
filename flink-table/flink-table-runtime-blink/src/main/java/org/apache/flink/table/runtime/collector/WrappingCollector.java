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

package org.apache.flink.table.runtime.collector;

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.util.Collector;

/**
 * A {@link Collector} that wraps another collector. An implementation can decide when to emit to the
 * wrapped collector.
 */
public abstract class WrappingCollector<T> extends AbstractRichFunction implements Collector<T> {

	private Collector<T> collector;

	/**
	 * Sets the current collector which is used to emit the final result.
	 */
	public void setCollector(Collector<T> collector) {
		this.collector = collector;
	}

	/**
	 * Outputs the final result to the wrapped collector.
	 */
	public void outputResult(T result) {
		this.collector.collect(result);
	}

	@Override
	public void close() {
		this.collector.close();
	}
}
