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

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.function.co.JoinWindowFunction;
import org.apache.flink.streaming.util.keys.FieldsKeySelector;
import org.apache.flink.streaming.util.keys.PojoKeySelector;

public class StreamJoinOperator<I1, I2> extends
		WindowDBOperator<I1, I2, StreamJoinOperator.JoinWindow<I1, I2>> {

	public StreamJoinOperator(DataStream<I1> input1, DataStream<I2> input2) {
		super(input1, input2);
	}

	@Override
	protected JoinWindow<I1, I2> createNextWindowOperator() {
		return new JoinWindow<I1, I2>(this);
	}

	public static class JoinWindow<I1, I2> {

		private StreamJoinOperator<I1, I2> op;

		private JoinWindow(StreamJoinOperator<I1, I2> operator) {
			this.op = operator;
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
			return new JoinPredicate<I1, I2>(op, FieldsKeySelector.getSelector(
					op.input1.getOutputType(), fields));
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
			return new JoinPredicate<I1, I2>(op, new PojoKeySelector<I1>(op.input1.getOutputType(),
					fields));
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
		private final KeySelector<I1, ?> keys1;

		private JoinPredicate(StreamJoinOperator<I1, I2> operator, KeySelector<I1, ?> keys1) {
			this.op = operator;
			this.keys1 = keys1;
		}

		/**
		 * Continues a temporal Join transformation and defines the
		 * {@link Tuple} fields of the second join {@link DataStream} that
		 * should be used as join keys.<br/>
		 * <b>Note: Fields can only be selected as join keys on Tuple
		 * DataStreams.</b><br/>
		 * 
		 * The resulting operator wraps each pair of joining elements into a
		 * {@link Tuple2}, with the element of the first input being the first
		 * field of the tuple and the element of the second input being the
		 * second field of the tuple.
		 *
		 * @param fields
		 *            The indexes of the Tuple fields of the second join
		 *            DataStream that should be used as keys.
		 * @return The joined data stream.
		 */
		public SingleOutputStreamOperator<Tuple2<I1, I2>, ?> equalTo(int... fields) {
			return createJoinOperator(FieldsKeySelector.getSelector(op.input2.getOutputType(),
					fields));
		}

		/**
		 * Continues a temporal Join transformation and defines the fields of
		 * the second join {@link DataStream} that should be used as join keys.<br/>
		 *
		 * The resulting operator wraps each pair of joining elements into a
		 * {@link Tuple2}, with the element of the first input being the first
		 * field of the tuple and the element of the second input being the
		 * second field of the tuple.
		 *
		 * @param fields
		 *            The fields of the second join DataStream that should be
		 *            used as keys.
		 * @return The joined data stream.
		 */
		public SingleOutputStreamOperator<Tuple2<I1, I2>, ?> equalTo(String... fields) {
			return createJoinOperator(new PojoKeySelector<I2>(op.input2.getOutputType(), fields));
		}

		/**
		 * Continues a temporal Join transformation and defines a
		 * {@link KeySelector} function for the second join {@link DataStream}
		 * .</br> The KeySelector function is called for each element of the
		 * second DataStream and extracts a single key value on which the
		 * DataStream is joined. </br>
		 * 
		 * The resulting operator wraps each pair of joining elements into a
		 * {@link Tuple2}, with the element of the first input being the first
		 * field of the tuple and the element of the second input being the
		 * second field of the tuple.
		 * 
		 * @param keySelector
		 *            The KeySelector function which extracts the key values
		 *            from the second DataStream on which it is joined.
		 * @return The joined data stream.
		 */
		public <K> SingleOutputStreamOperator<Tuple2<I1, I2>, ?> equalTo(
				KeySelector<I2, K> keySelector) {
			return createJoinOperator(keySelector);
		}

		protected SingleOutputStreamOperator<Tuple2<I1, I2>, ?> createJoinOperator(
				KeySelector<I2, ?> keys2) {

			JoinWindowFunction<I1, I2> joinWindowFunction = new JoinWindowFunction<I1, I2>(keys1,
					keys2);
			return op.input1.connect(op.input2).addGeneralWindowJoin(joinWindowFunction,
					op.windowSize, op.slideInterval, op.timeStamp1, op.timeStamp2);
		}
	}

}
