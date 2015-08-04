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

package org.apache.flink.streaming.api.datastream;

import java.util.List;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.typeutils.TypeInfoParser;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;

/**
 * The iterative data stream represents the start of an iteration in a
 * {@link DataStream}.
 * 
 * @param <IN>
 *            Type of the DataStream
 */
public class IterativeDataStream<IN> extends
		SingleOutputStreamOperator<IN, IterativeDataStream<IN>> {
	
	protected boolean closed = false;

	static Integer iterationCount = 0;
	
	protected IterativeDataStream(DataStream<IN> dataStream, long maxWaitTime) {
		super(dataStream);
		setBufferTimeout(dataStream.environment.getBufferTimeout());
		iterationID = iterationCount;
		iterationCount++;
		iterationWaitTime = maxWaitTime;
	}

	/**
	 * Closes the iteration. This method defines the end of the iterative
	 * program part that will be fed back to the start of the iteration. </br>
	 * </br>A common usage pattern for streaming iterations is to use output
	 * splitting to send a part of the closing data stream to the head. Refer to
	 * {@link DataStream#split(org.apache.flink.streaming.api.collector.selector.OutputSelector)}
	 * for more information.
	 * 
	 * @param feedbackStream
	 *            {@link DataStream} that will be used as input to the iteration
	 *            head.
	 * @param keepPartitioning
	 *            If true the feedback partitioning will be kept as it is (not
	 *            changed to match the input of the iteration head)
	 * @return The feedback stream.
	 * 
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public DataStream<IN> closeWith(DataStream<IN> iterationTail, boolean keepPartitioning) {
		
		if (closed) {
			throw new IllegalStateException(
					"An iterative data stream can only be closed once. Use union to close with multiple stream.");
		}
		closed = true;
		
		streamGraph.addIterationTail((List) iterationTail.unionedStreams, iterationID,
				keepPartitioning);

		return iterationTail;
	}
	
	/**
	 * Closes the iteration. This method defines the end of the iterative
	 * program part that will be fed back to the start of the iteration. </br>
	 * </br>A common usage pattern for streaming iterations is to use output
	 * splitting to send a part of the closing data stream to the head. Refer to
	 * {@link DataStream#split(org.apache.flink.streaming.api.collector.selector.OutputSelector)}
	 * for more information.
	 * 
	 * 
	 * @param feedbackStream
	 *            {@link DataStream} that will be used as input to the
	 *            iteration head.
	 * @return The feedback stream.
	 * 
	 */
	public DataStream<IN> closeWith(DataStream<IN> iterationTail) {
		return closeWith(iterationTail,false);
	}

	/**
	 * Changes the feedback type of the iteration and allows the user to apply
	 * co-transformations on the input and feedback stream, as in a
	 * {@link ConnectedDataStream}.
	 * <p>
	 * For type safety the user needs to define the feedback type
	 * 
	 * @param feedbackTypeString
	 *            String describing the type information of the feedback stream.
	 * @return A {@link ConnectedIterativeDataStream}.
	 */
	public <F> ConnectedIterativeDataStream<IN, F> withFeedbackType(String feedbackTypeString) {
		return withFeedbackType(TypeInfoParser.<F> parse(feedbackTypeString));
	}

	/**
	 * Changes the feedback type of the iteration and allows the user to apply
	 * co-transformations on the input and feedback stream, as in a
	 * {@link ConnectedDataStream}.
	 * <p>
	 * For type safety the user needs to define the feedback type
	 * 
	 * @param feedbackTypeClass
	 *            Class of the elements in the feedback stream.
	 * @return A {@link ConnectedIterativeDataStream}.
	 */
	public <F> ConnectedIterativeDataStream<IN, F> withFeedbackType(Class<F> feedbackTypeClass) {
		return withFeedbackType(TypeExtractor.getForClass(feedbackTypeClass));
	}

	/**
	 * Changes the feedback type of the iteration and allows the user to apply
	 * co-transformations on the input and feedback stream, as in a
	 * {@link ConnectedDataStream}.
	 * <p>
	 * For type safety the user needs to define the feedback type
	 * 
	 * @param feedbackType
	 *            The type information of the feedback stream.
	 * @return A {@link ConnectedIterativeDataStream}.
	 */
	public <F> ConnectedIterativeDataStream<IN, F> withFeedbackType(TypeInformation<F> feedbackType) {
		return new ConnectedIterativeDataStream<IN, F>(new IterativeDataStream<IN>(this,
				iterationWaitTime), feedbackType);
	}
	
	/**
	 * The {@link ConnectedIterativeDataStream} represent a start of an
	 * iterative part of a streaming program, where the original input of the
	 * iteration and the feedback of the iteration are connected as in a
	 * {@link ConnectedDataStream}.
	 * <p>
	 * The user can distinguish between the two inputs using co-transformation,
	 * thus eliminating the need for mapping the inputs and outputs to a common
	 * type.
	 * 
	 * @param <I>
	 *            Type of the input of the iteration
	 * @param <F>
	 *            Type of the feedback of the iteration
	 */
	public static class ConnectedIterativeDataStream<I, F> extends ConnectedDataStream<I, F>{

		private IterativeDataStream<I> input;
		private TypeInformation<F> feedbackType;

		public ConnectedIterativeDataStream(IterativeDataStream<I> input, TypeInformation<F> feedbackType) {
			super(input, null);
			this.input = input;
			this.feedbackType = feedbackType;
		}
		
		@Override
		public TypeInformation<F> getType2() {
			return feedbackType;
		}
		
		@Override
		public <OUT> SingleOutputStreamOperator<OUT, ?> transform(String functionName,
				TypeInformation<OUT> outTypeInfo, TwoInputStreamOperator<I, F, OUT> operator) {

			@SuppressWarnings({ "unchecked", "rawtypes" })
			SingleOutputStreamOperator<OUT, ?> returnStream = new SingleOutputStreamOperator(
					input.environment, outTypeInfo, operator);

			input.streamGraph.addCoOperator(returnStream.getId(), operator, input.getType(),
					feedbackType, outTypeInfo, functionName);

			input.connectGraph(input, returnStream.getId(), 1);
			
			input.addIterationSource(returnStream, feedbackType);

			return returnStream;
		}
		
		/**
		 * Closes the iteration. This method defines the end of the iterative
		 * program part that will be fed back to the start of the iteration as
		 * the second input in the {@link ConnectedDataStream}.
		 * 
		 * @param feedbackStream
		 *            {@link DataStream} that will be used as second input to
		 *            the iteration head.
		 * @return The feedback stream.
		 * 
		 */
		@SuppressWarnings({ "rawtypes", "unchecked" })
		public DataStream<F> closeWith(DataStream<F> feedbackStream) {
			if (input.closed) {
				throw new IllegalStateException(
						"An iterative data stream can only be closed once. Use union to close with multiple stream.");
			}
			input.closed = true;
			
			input.streamGraph.addIterationTail((List) feedbackStream.unionedStreams,
					input.iterationID, true);
			return feedbackStream;
		}
		
		private UnsupportedOperationException groupingException = new UnsupportedOperationException(
				"Cannot change the input partitioning of an iteration head directly. Apply the partitioning on the input and feedback streams instead.");
		
		@Override
		public ConnectedDataStream<I, F> groupBy(int keyPosition1, int keyPosition2) {throw groupingException;}
		
		@Override
		public ConnectedDataStream<I, F> groupBy(int[] keyPositions1, int[] keyPositions2) {throw groupingException;}
		
		@Override
		public ConnectedDataStream<I, F> groupBy(String field1, String field2) {throw groupingException;}
		
		@Override
		public ConnectedDataStream<I, F> groupBy(String[] fields1, String[] fields2) {throw groupingException;}
		
		@Override
		public ConnectedDataStream<I, F> groupBy(KeySelector<I, ?> keySelector1,KeySelector<F, ?> keySelector2) {throw groupingException;}
		
		@Override
		public ConnectedDataStream<I, F> partitionByHash(int keyPosition1, int keyPosition2) {throw groupingException;}
		
		@Override
		public ConnectedDataStream<I, F> partitionByHash(int[] keyPositions1, int[] keyPositions2) {throw groupingException;}
		
		@Override
		public ConnectedDataStream<I, F> partitionByHash(String field1, String field2) {throw groupingException;}
		
		@Override
		public ConnectedDataStream<I, F> partitionByHash(String[] fields1, String[] fields2) {throw groupingException;}
		
		@Override
		public ConnectedDataStream<I, F> partitionByHash(KeySelector<I, ?> keySelector1, KeySelector<F, ?> keySelector2) {throw groupingException;}
		
	}
}
