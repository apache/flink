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

package org.apache.flink.streaming.api.datastream.temporal;

import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.Keys;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.co.JoinWindowFunction;
import org.apache.flink.streaming.api.operators.co.CoStreamWindow;
import org.apache.flink.streaming.util.keys.KeySelectorUtil;

public class StreamJoinOperator<I1, I2> extends
		TemporalOperator<I1, I2, StreamJoinOperator.JoinWindow<I1, I2>> {

	public StreamJoinOperator(DataStream<I1> input1, DataStream<I2> input2) {
		super(input1, input2);
	}

	@Override
	protected JoinWindow<I1, I2> createNextWindowOperator() {
		return new JoinWindow<I1, I2>(this);
	}

	public static class JoinWindow<I1, I2> implements TemporalWindow<JoinWindow<I1, I2>> {

		private StreamJoinOperator<I1, I2> op;
		private TypeInformation<I1> type1;

		private JoinWindow(StreamJoinOperator<I1, I2> operator) {
			this.op = operator;
			this.type1 = op.input1.getType();
		}

		/**
		 * Continues a temporal Join transformation. <br/>
		 * Defines the {@link Tuple} fields of the first join {@link DataStream}
		 * that should be used as join keys.<br/>
		 * <b>Note: Fields can only be selected as join keys on Tuple
		 * DataStreams.</b><br/>
		 * 
		 * @param fields
		 *            The indexes of the other Tuple fields of the first join
		 *            DataStreams that should be used as keys.
		 * @return An incomplete Join transformation. Call
		 *         {@link JoinPredicate#equalTo} to continue the Join.
		 */
		public JoinPredicate<I1, I2> where(int... fields) {
			return new JoinPredicate<I1, I2>(op, KeySelectorUtil.getSelectorForKeys(
					new Keys.ExpressionKeys<I1>(fields, type1), type1, op.input1.getExecutionEnvironment().getConfig()));
		}

		/**
		 * Continues a temporal join transformation. <br/>
		 * Defines the fields of the first join {@link DataStream} that should
		 * be used as grouping keys. Fields are the names of member fields of
		 * the underlying type of the data stream.
		 * 
		 * @param fields
		 *            The fields of the first join DataStream that should be
		 *            used as keys.
		 * @return An incomplete Join transformation. Call
		 *         {@link JoinPredicate#equalTo} to continue the Join.
		 */
		public JoinPredicate<I1, I2> where(String... fields) {
			return new JoinPredicate<I1, I2>(op, KeySelectorUtil.getSelectorForKeys(
					new Keys.ExpressionKeys<I1>(fields, type1), type1, op.input1.getExecutionEnvironment().getConfig()));
		}

		/**
		 * Continues a temporal Join transformation and defines a
		 * {@link KeySelector} function for the first join {@link DataStream}
		 * .</br> The KeySelector function is called for each element of the
		 * first DataStream and extracts a single key value on which the
		 * DataStream is joined. </br>
		 * 
		 * @param keySelector
		 *            The KeySelector function which extracts the key values
		 *            from the DataStream on which it is joined.
		 * @return An incomplete Join transformation. Call
		 *         {@link JoinPredicate#equalTo} to continue the Join.
		 */
		public <K> JoinPredicate<I1, I2> where(KeySelector<I1, K> keySelector) {
			return new JoinPredicate<I1, I2>(op, keySelector);
		}

		@Override
		public JoinWindow<I1, I2> every(long length, TimeUnit timeUnit) {
			return every(timeUnit.toMillis(length));
		}

		@Override
		public JoinWindow<I1, I2> every(long length) {
			op.slideInterval = length;
			return this;
		}

		// ----------------------------------------------------------------------------------------

	}

	/**
	 * Intermediate step of a temporal Join transformation. <br/>
	 * To continue the Join transformation, select the join key of the second
	 * input {@link DataStream} by calling {@link JoinPredicate#equalTo}
	 * 
	 */
	public static class JoinPredicate<I1, I2> {

		private StreamJoinOperator<I1, I2> op;
		private KeySelector<I1, ?> keys1;
		private KeySelector<I2, ?> keys2;
		private TypeInformation<I2> type2;

		private JoinPredicate(StreamJoinOperator<I1, I2> operator, KeySelector<I1, ?> keys1) {
			this.op = operator;
			this.keys1 = keys1;
			this.type2 = op.input2.getType();
		}

		/**
		 * Creates a temporal Join transformation and defines the {@link Tuple}
		 * fields of the second join {@link DataStream} that should be used as
		 * join keys.<br/>
		 * </p> The resulting operator wraps each pair of joining elements in a
		 * Tuple2<I1,I2>(first, second). To use a different wrapping function
		 * use {@link JoinedStream#with(JoinFunction)}
		 * 
		 * @param fields
		 *            The indexes of the Tuple fields of the second join
		 *            DataStream that should be used as keys.
		 * @return A streaming join operator. Call {@link JoinedStream#with} to
		 *         apply a custom wrapping
		 */
		public JoinedStream<I1, I2> equalTo(int... fields) {
			keys2 = KeySelectorUtil.getSelectorForKeys(new Keys.ExpressionKeys<I2>(fields, type2),
					type2, op.input1.getExecutionEnvironment().getConfig());
			return createJoinOperator();
		}

		/**
		 * Creates a temporal Join transformation and defines the fields of the
		 * second join {@link DataStream} that should be used as join keys. </p>
		 * The resulting operator wraps each pair of joining elements in a
		 * Tuple2<I1,I2>(first, second). To use a different wrapping function
		 * use {@link JoinedStream#with(JoinFunction)}
		 * 
		 * @param fields
		 *            The fields of the second join DataStream that should be
		 *            used as keys.
		 * @return A streaming join operator. Call {@link JoinedStream#with} to
		 *         apply a custom wrapping
		 */
		public JoinedStream<I1, I2> equalTo(String... fields) {
			this.keys2 = KeySelectorUtil.getSelectorForKeys(new Keys.ExpressionKeys<I2>(fields,
					type2), type2, op.input1.getExecutionEnvironment().getConfig());
			return createJoinOperator();
		}

		/**
		 * Creates a temporal Join transformation and defines a
		 * {@link KeySelector} function for the second join {@link DataStream}
		 * .</br> The KeySelector function is called for each element of the
		 * second DataStream and extracts a single key value on which the
		 * DataStream is joined. </p> The resulting operator wraps each pair of
		 * joining elements in a Tuple2<I1,I2>(first, second). To use a
		 * different wrapping function use
		 * {@link JoinedStream#with(JoinFunction)}
		 * 
		 * 
		 * @param keySelector
		 *            The KeySelector function which extracts the key values
		 *            from the second DataStream on which it is joined.
		 * @return A streaming join operator. Call {@link JoinedStream#with} to
		 *         apply a custom wrapping
		 */
		public <K> JoinedStream<I1, I2> equalTo(KeySelector<I2, K> keySelector) {
			this.keys2 = keySelector;
			return createJoinOperator();
		}

		private JoinedStream<I1, I2> createJoinOperator() {

			JoinFunction<I1, I2, Tuple2<I1, I2>> joinFunction = new DefaultJoinFunction<I1, I2>();

			JoinWindowFunction<I1, I2, Tuple2<I1, I2>> joinWindowFunction = getJoinWindowFunction(
					joinFunction, this);

			TypeInformation<Tuple2<I1, I2>> outType = new TupleTypeInfo<Tuple2<I1, I2>>(
					op.input1.getType(), op.input2.getType());

			return new JoinedStream<I1, I2>(this, op.input1
					.groupBy(keys1)
					.connect(op.input2.groupBy(keys2))
					.addGeneralWindowCombine(joinWindowFunction, outType, op.windowSize,
							op.slideInterval, op.timeStamp1, op.timeStamp2));
		}
	}

	public static class JoinedStream<I1, I2> extends
			SingleOutputStreamOperator<Tuple2<I1, I2>, JoinedStream<I1, I2>> {
		private final JoinPredicate<I1, I2> predicate;

		private JoinedStream(JoinPredicate<I1, I2> predicate, DataStream<Tuple2<I1, I2>> ds) {
			super(ds);
			this.predicate = predicate;
		}

		/**
		 * Completes a stream join. </p> The resulting operator wraps each pair
		 * of joining elements using the user defined {@link JoinFunction}
		 * 
		 * @return The joined data stream.
		 */
		@SuppressWarnings("unchecked")
		public <OUT> SingleOutputStreamOperator<OUT, ?> with(JoinFunction<I1, I2, OUT> joinFunction) {

			TypeInformation<OUT> outType = TypeExtractor.getJoinReturnTypes(joinFunction,
					predicate.op.input1.getType(), predicate.op.input2.getType());

			CoStreamWindow<I1, I2, OUT> operator = new CoStreamWindow<I1, I2, OUT>(
					getJoinWindowFunction(joinFunction, predicate), predicate.op.windowSize,
					predicate.op.slideInterval, predicate.op.timeStamp1, predicate.op.timeStamp2);

			streamGraph.setOperator(id, operator);

			return ((SingleOutputStreamOperator<OUT, ?>) this).returns(outType);
		}
	}

	public static final class DefaultJoinFunction<I1, I2> implements
			JoinFunction<I1, I2, Tuple2<I1, I2>> {

		private static final long serialVersionUID = 1L;
		private final Tuple2<I1, I2> outTuple = new Tuple2<I1, I2>();

		@Override
		public Tuple2<I1, I2> join(I1 first, I2 second) throws Exception {
			outTuple.f0 = first;
			outTuple.f1 = second;
			return outTuple;
		}
	}

	private static <I1, I2, OUT> JoinWindowFunction<I1, I2, OUT> getJoinWindowFunction(
			JoinFunction<I1, I2, OUT> joinFunction, JoinPredicate<I1, I2> predicate) {
		return new JoinWindowFunction<I1, I2, OUT>(predicate.keys1, predicate.keys2, joinFunction);
	}
}
