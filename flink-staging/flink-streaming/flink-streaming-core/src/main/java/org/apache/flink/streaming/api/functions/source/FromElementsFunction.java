/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.functions.source;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

import org.apache.flink.configuration.Configuration;

public class FromElementsFunction<T> extends RichSourceFunction<T> {
	private static final long serialVersionUID = 1L;

	private transient Iterator<T> iterator;

	private Iterable<T> iterable;

	public FromElementsFunction(T... elements) {
		this.iterable = Arrays.asList(elements);
	}

	public FromElementsFunction(Collection<T> elements) {
		this.iterable = elements;
	}

	public FromElementsFunction(Iterable<T> elements) {
		this.iterable = elements;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		iterator = iterable.iterator();
	}

	@Override
	public boolean reachedEnd() throws Exception {
		return !iterator.hasNext();
	}

	@Override
	public T next() throws Exception {
		return iterator.next();
	}

}
