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

package org.apache.flink.test.util;

import org.apache.flink.api.common.io.GenericInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.runtime.operators.testutils.UniformIntTupleGenerator;

import java.io.IOException;

/**
 * Generates a series of integer 2-tuples.
 */
public class UniformIntTupleGeneratorInputFormat extends GenericInputFormat<Tuple2<Integer, Integer>> {
	private final int keyTotal;
	private final int valueTotal;
	private int valueCount = 0;
	private UniformIntTupleGenerator generator;

	public UniformIntTupleGeneratorInputFormat(int numKeys, int numVals) {
		keyTotal = numKeys;
		valueTotal = numVals;
	}

	@Override
	public void open(GenericInputSplit split) throws IOException {
		super.open(split);
		this.generator = new UniformIntTupleGenerator(keyTotal, valueTotal, false);
	}

	@Override
	public boolean reachedEnd() throws IOException {
		return valueCount >= valueTotal;
	}

	@Override
	public Tuple2<Integer, Integer> nextRecord(Tuple2<Integer, Integer> reuse) throws IOException {
		valueCount += 1;
		return generator.next();
	}
}
