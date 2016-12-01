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
package org.apache.flink.util;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.OutputTag;

import java.io.Serializable;

/**
 * Wrapper to collect normal output and sideoutput. Provide Wrapper to sideoutput element
 * without break 1.x API compatability
 * <code>
 *     Example:
 *     	flatMap(T value, Collector&lt;OUT&gt; collector) {
 *     	    CollectorWrapper&lt;OUT&gt; wrapper = CollectorWrapper&lt;&gt;(collector);
 *     	    wrapper.collect(...)// same as collector.collect(..)
 *     	    wrapper.collect(OutputTag&lt;SIDEOUT&gt; sideOutputTag, ...) //sideOutput
 *     	}
 * </code>
 */
@PublicEvolving
public class CollectorWrapper<T> implements RichCollector<T> , Serializable{

	private final RichCollector<T> collector;

	/**
	 * side output wrapper, allow user do sideoutputs
	 * @param collector public collector interface
     */
	public CollectorWrapper(final Collector<T> collector) {
		this.collector = (RichCollector<T>) collector;
	}

	/**
	 * side output collect interface
	 * @param outputTag side output outputTag
	 * @param value side output element
	 * @param <S> sideoutput type
     */
	@Override
	public <S> void collect(final OutputTag<S> outputTag, final S value) {
		collector.collect(outputTag, value);
	}

	/**
	 * backward compatabile, output non sideoutputs
	 * @param record The record to collect.
     */
	@Override
	public void collect(T record) {
		this.collector.collect(record);
	}

	/**
	 * close collector
	 */
	@Override
	public void close() {
		this.collector.close();
	}
}
