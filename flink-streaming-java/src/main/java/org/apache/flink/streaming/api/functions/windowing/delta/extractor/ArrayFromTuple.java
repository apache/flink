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
 * Converts a Tuple to an Object-Array. The field which should be included in
 * the array can selected and reordered as needed.
 */
@Internal
public class ArrayFromTuple implements Extractor<Tuple, Object[]> {

	private static final long serialVersionUID = -6076121226427616818L;
	int[] order = null;

	/**
	 * Using this constructor the extractor will convert the whole tuple (all
	 * fields in the original order) to an array.
	 */
	public ArrayFromTuple() {
		// noting to do
	}

	/**
	 * Using this constructor the extractor will combine the fields as specified
	 * in the indexes parameter in an object array.
	 *
	 * @param indexes
	 *            the field ids (enumerated from 0)
	 */
	public ArrayFromTuple(int... indexes) {
		this.order = indexes;
	}

	@Override
	public Object[] extract(Tuple in) {
		Object[] output;

		if (order == null) {
			// copy the whole tuple
			output = new Object[in.getArity()];
			for (int i = 0; i < in.getArity(); i++) {
				output[i] = in.getField(i);
			}
		} else {
			// copy user specified order
			output = new Object[order.length];
			for (int i = 0; i < order.length; i++) {
				output[i] = in.getField(order[i]);
			}
		}

		return output;
	}

}
