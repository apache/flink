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

package org.apache.flink.streaming.api.datastream;

import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.CrossOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.function.co.CrossWindowFunction;

public class StreamCrossOperator<I1, I2> extends
		TemporalOperator<I1, I2, StreamCrossOperator.CrossWindow<I1, I2>> {

	public StreamCrossOperator(DataStream<I1> input1, DataStream<I2> input2) {
		super(input1, input2);
	}

	@Override
	protected CrossWindow<I1, I2> createNextWindowOperator() {
		return new CrossWindow<I1, I2>(this);
	}

	public static class CrossWindow<I1, I2> {

		private StreamCrossOperator<I1, I2> op;

		public CrossWindow(StreamCrossOperator<I1, I2> operator) {
			this.op = operator;
		}

		public <F> F clean(F f) {
			if (op.input1.getExecutionEnvironment().getConfig().isClosureCleanerEnabled()) {
				ClosureCleaner.clean(f, true);
			}
			ClosureCleaner.ensureSerializable(f);
			return f;
		}

		/**
		 * Finalizes a temporal Cross transformation by applying a
		 * {@link CrossFunction} to each pair of crossed elements.<br/>
		 * Each CrossFunction call returns exactly one element.
		 * 
		 * @param function
		 *            The CrossFunction that is called for each pair of crossed
		 *            elements.
		 * @return A CrossOperator that represents the crossed result DataStream
		 * 
		 * @see CrossFunction
		 * @see DataSet
		 */
		public <R> SingleOutputStreamOperator<R, ?> with(CrossFunction<I1, I2, R> function) {
			return createCrossOperator(function);
		}

		/**
		 * Finalizes a temporal Cross transformation by emitting all pairs in a
		 * new Tuple2.
		 * 
		 * @return A CrossOperator that represents the crossed result DataStream
		 */
		public SingleOutputStreamOperator<Tuple2<I1, I2>, ?> withDefault() {
			return createCrossOperator(new CrossOperator.DefaultCrossFunction<I1, I2>());
		}

		protected <R> SingleOutputStreamOperator<R, ?> createCrossOperator(
				CrossFunction<I1, I2, R> function) {

			TypeInformation<R> outTypeInfo = TypeExtractor.getCrossReturnTypes(function,
					op.input1.getType(), op.input2.getType());

			CrossWindowFunction<I1, I2, R> crossWindowFunction = new CrossWindowFunction<I1, I2, R>(
					clean(function));

			return op.input1.connect(op.input2).addGeneralWindowCombine(crossWindowFunction,
					outTypeInfo, op.windowSize, op.slideInterval, op.timeStamp1, op.timeStamp2);

		}

	}

}
