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

package org.apache.flink.api.java.operators.translation;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * Needed to wrap tuples to {@code Tuple2<key, value>} pairs for combine method of group reduce with key selector function.
 */
@Internal
public class TupleWrappingCollector<IN, K> implements Collector<IN>, java.io.Serializable {

	private static final long serialVersionUID = 1L;

	private final TupleUnwrappingIterator<IN, K> tui;
	private final Tuple2<K, IN> outTuple;

	private Collector<Tuple2<K, IN>> wrappedCollector;

	public TupleWrappingCollector(TupleUnwrappingIterator<IN, K> tui) {
		this.tui = tui;
		this.outTuple = new Tuple2<K, IN>();
	}

	public void set(Collector<Tuple2<K, IN>> wrappedCollector) {
			this.wrappedCollector = wrappedCollector;
	}

	@Override
	public void close() {
		this.wrappedCollector.close();
	}

	@Override
	public void collect(IN record) {
		this.outTuple.f0 = this.tui.getLastKey();
		this.outTuple.f1 = record;
		this.wrappedCollector.collect(outTuple);
	}

}
