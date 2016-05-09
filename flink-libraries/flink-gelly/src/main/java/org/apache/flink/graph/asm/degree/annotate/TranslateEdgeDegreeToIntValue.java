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

package org.apache.flink.graph.asm.degree.annotate;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.asm.translate.TranslateFunction;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.LongValue;

/**
 * Translate the edge degree returned by the degree annotation functions from
 * {@link LongValue} to {@link IntValue}.
 *
 * @param <T> edge value type
 */
public class TranslateEdgeDegreeToIntValue<T>
implements TranslateFunction<Tuple2<T, LongValue>, Tuple2<T, IntValue>> {

	@Override
	public Tuple2<T, IntValue> translate(Tuple2<T, LongValue> value, Tuple2<T, IntValue> reuse) throws Exception {
		long val = value.f1.getValue();

		if (val > Integer.MAX_VALUE) {
			throw new RuntimeException("LongValue input overflows IntValue output");
		}

		if (reuse == null) {
			reuse = new Tuple2<>(null, new IntValue());
		}

		reuse.f0 = value.f0;
		reuse.f1.setValue((int) val);
		return reuse;
	}
}
