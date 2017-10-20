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

package org.apache.flink.streaming.api.functions.windowing.delta.extractor;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.tuple.Tuple;

/**
 * Extracts one or more fields of the type Double from a tuple and puts them into a new double[].
 */
@Internal
public class FieldsFromTuple implements Extractor<Tuple, double[]> {

	private static final long serialVersionUID = -2554079091050273761L;

	int[] indexes;

	/**
	 * Extracts one or more fields of the type Double from a tuple and puts
	 * them into a new double[] (in the specified order).
	 *
	 * @param indexes
	 *            The indexes of the fields to be extracted.
	 */
	public FieldsFromTuple(int... indexes) {
		this.indexes = indexes;
	}

	@Override
	public double[] extract(Tuple in) {
		double[] out = new double[indexes.length];
		for (int i = 0; i < indexes.length; i++) {
			out[i] = (Double) in.getField(indexes[i]);
		}
		return out;
	}
}
