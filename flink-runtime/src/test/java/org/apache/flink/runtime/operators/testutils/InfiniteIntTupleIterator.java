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

package org.apache.flink.runtime.operators.testutils;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.MutableObjectIterator;

/**
 * A simple iterator that returns an infinite amount of (0, 0) tuples.
 */
public class InfiniteIntTupleIterator implements MutableObjectIterator<Tuple2<Integer, Integer>> {
	
	@Override
	public Tuple2<Integer, Integer> next(Tuple2<Integer, Integer> reuse) {
		return next();
	}

	@Override
	public Tuple2<Integer, Integer> next() {
		return new Tuple2<Integer, Integer>(0, 0);
	}
}
