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

package org.apache.flink.api.streaming.scala;

import java.io.Serializable;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializerBase;
import org.apache.flink.streaming.api.functions.aggregation.AggregationFunction;
import org.apache.flink.streaming.api.functions.aggregation.ComparableAggregator;
import org.apache.flink.streaming.api.functions.aggregation.SumFunction;

import scala.Product;

public class ScalaStreamingAggregator<IN extends Product> implements Serializable {

	private static final long serialVersionUID = 1L;

	TupleSerializerBase<IN> serializer;
	Object[] fields;
	int length;
	int position;

	public ScalaStreamingAggregator(TypeSerializer<IN> serializer, int pos) {
		this.serializer = (TupleSerializerBase<IN>) serializer;
		this.length = this.serializer.getArity();
		this.fields = new Object[this.length];
		this.position = pos;
	}

	public class Sum extends AggregationFunction<IN> {
		private static final long serialVersionUID = 1L;
		SumFunction sumFunction;

		public Sum(SumFunction func) {
			super(ScalaStreamingAggregator.this.position);
			this.sumFunction = func;
		}

		@Override
		public IN reduce(IN value1, IN value2) throws Exception {
			for (int i = 0; i < length; i++) {
				fields[i] = value2.productElement(i);
			}

			fields[position] = sumFunction.add(fields[position], value1.productElement(position));

			return serializer.createInstance(fields);
		}
	}

	public class ProductComparableAggregator extends ComparableAggregator<IN> {

		private static final long serialVersionUID = 1L;

		public ProductComparableAggregator(AggregationFunction.AggregationType aggregationType,
				boolean first) {
			super(ScalaStreamingAggregator.this.position, aggregationType, first);
		}

		@SuppressWarnings("unchecked")
		@Override
		public IN reduce(IN value1, IN value2) throws Exception {
			Object v1 = value1.productElement(position);
			Object v2 = value2.productElement(position);

			int c = comparator.isExtremal((Comparable<Object>) v1, v2);

			if (byAggregate) {
				if (c == 1) {
					return value1;
				}
				if (first) {
					if (c == 0) {
						return value1;
					}
				}

				return value2;
			} else {
				for (int i = 0; i < length; i++) {
					fields[i] = value2.productElement(i);
				}

				if (c == 1) {
					fields[position] = v1;
				}

				return serializer.createInstance(fields);
			}
		}

	}

}
