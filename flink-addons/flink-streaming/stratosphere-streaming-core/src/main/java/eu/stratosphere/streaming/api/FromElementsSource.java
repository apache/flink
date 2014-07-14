/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.streaming.api;

import java.util.Arrays;
import java.util.Collection;

import eu.stratosphere.api.java.tuple.Tuple1;
import eu.stratosphere.util.Collector;

public class FromElementsSource<T> extends SourceFunction<Tuple1<T>> {
	private static final long serialVersionUID = 1L;

	Iterable<T> iterable;
	Tuple1<T> outTuple = new Tuple1<T>();

	public FromElementsSource(T... elements) {
		this.iterable = (Iterable<T>) Arrays.asList(elements);
	}

	public FromElementsSource(Collection<T> elements) {
		this.iterable = (Iterable<T>) elements;
	}

	@Override
	public void invoke(Collector<Tuple1<T>> collector) throws Exception {
		for (T element : iterable) {
			outTuple.f0 = element;
			collector.collect(outTuple);
		}
	}

}
