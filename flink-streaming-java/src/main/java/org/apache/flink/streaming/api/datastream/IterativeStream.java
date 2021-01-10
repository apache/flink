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

import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.transformations.CoFeedbackTransformation;
import org.apache.flink.streaming.api.transformations.FeedbackTransformation;
import org.apache.flink.util.OutputTag;

import java.util.Collection;

/**
 * The iterative data stream represents the start of an iteration in a {@link DataStream}.
 *
 * @param <T> Type of the elements in this Stream
 */
@PublicEvolving
public class IterativeStream<T> extends SingleOutputStreamOperator<T> {

    // We store these so that we can create a co-iteration if we need to
    private DataStream<T> originalInput;
    private long maxWaitTime;

    protected IterativeStream(DataStream<T> dataStream, long maxWaitTime) {
        super(
                dataStream.getExecutionEnvironment(),
                new FeedbackTransformation<>(dataStream.getTransformation(), maxWaitTime));
        this.originalInput = dataStream;
        this.maxWaitTime = maxWaitTime;
        setBufferTimeout(dataStream.environment.getBufferTimeout());
    }

    /**
     * Closes the iteration. This method defines the end of the iterative program part that will be
     * fed back to the start of the iteration.
     *
     * <p>A common usage pattern for streaming iterations is to use output splitting to send a part
     * of the closing data stream to the head. Refer to {@link
     * ProcessFunction.Context#output(OutputTag, Object)} for more information.
     *
     * @param feedbackStream {@link DataStream} that will be used as input to the iteration head.
     * @return The feedback stream.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public DataStream<T> closeWith(DataStream<T> feedbackStream) {

        Collection<Transformation<?>> predecessors =
                feedbackStream.getTransformation().getTransitivePredecessors();

        if (!predecessors.contains(this.transformation)) {
            throw new UnsupportedOperationException(
                    "Cannot close an iteration with a feedback DataStream that does not originate from said iteration.");
        }

        ((FeedbackTransformation) getTransformation())
                .addFeedbackEdge(feedbackStream.getTransformation());

        return feedbackStream;
    }

    /**
     * Changes the feedback type of the iteration and allows the user to apply co-transformations on
     * the input and feedback stream, as in a {@link ConnectedStreams}.
     *
     * <p>For type safety the user needs to define the feedback type
     *
     * @param feedbackTypeClass Class of the elements in the feedback stream.
     * @return A {@link ConnectedIterativeStreams}.
     */
    public <F> ConnectedIterativeStreams<T, F> withFeedbackType(Class<F> feedbackTypeClass) {
        return withFeedbackType(TypeInformation.of(feedbackTypeClass));
    }

    /**
     * Changes the feedback type of the iteration and allows the user to apply co-transformations on
     * the input and feedback stream, as in a {@link ConnectedStreams}.
     *
     * <p>For type safety the user needs to define the feedback type
     *
     * @param feedbackTypeHint Class of the elements in the feedback stream.
     * @return A {@link ConnectedIterativeStreams}.
     */
    public <F> ConnectedIterativeStreams<T, F> withFeedbackType(TypeHint<F> feedbackTypeHint) {
        return withFeedbackType(TypeInformation.of(feedbackTypeHint));
    }

    /**
     * Changes the feedback type of the iteration and allows the user to apply co-transformations on
     * the input and feedback stream, as in a {@link ConnectedStreams}.
     *
     * <p>For type safety the user needs to define the feedback type
     *
     * @param feedbackType The type information of the feedback stream.
     * @return A {@link ConnectedIterativeStreams}.
     */
    public <F> ConnectedIterativeStreams<T, F> withFeedbackType(TypeInformation<F> feedbackType) {
        return new ConnectedIterativeStreams<>(originalInput, feedbackType, maxWaitTime);
    }

    /**
     * The {@link ConnectedIterativeStreams} represent a start of an iterative part of a streaming
     * program, where the original input of the iteration and the feedback of the iteration are
     * connected as in a {@link ConnectedStreams}.
     *
     * <p>The user can distinguish between the two inputs using co-transformation, thus eliminating
     * the need for mapping the inputs and outputs to a common type.
     *
     * @param <I> Type of the input of the iteration
     * @param <F> Type of the feedback of the iteration
     */
    @Public
    public static class ConnectedIterativeStreams<I, F> extends ConnectedStreams<I, F> {

        private CoFeedbackTransformation<F> coFeedbackTransformation;

        public ConnectedIterativeStreams(
                DataStream<I> input, TypeInformation<F> feedbackType, long waitTime) {
            super(
                    input.getExecutionEnvironment(),
                    input,
                    new DataStream<>(
                            input.getExecutionEnvironment(),
                            new CoFeedbackTransformation<>(
                                    input.getParallelism(), feedbackType, waitTime)));
            this.coFeedbackTransformation =
                    (CoFeedbackTransformation<F>) getSecondInput().getTransformation();
        }

        /**
         * Closes the iteration. This method defines the end of the iterative program part that will
         * be fed back to the start of the iteration as the second input in the {@link
         * ConnectedStreams}.
         *
         * @param feedbackStream {@link DataStream} that will be used as second input to the
         *     iteration head.
         * @return The feedback stream.
         */
        public DataStream<F> closeWith(DataStream<F> feedbackStream) {

            Collection<Transformation<?>> predecessors =
                    feedbackStream.getTransformation().getTransitivePredecessors();

            if (!predecessors.contains(this.coFeedbackTransformation)) {
                throw new UnsupportedOperationException(
                        "Cannot close an iteration with a feedback DataStream that does not originate from said iteration.");
            }

            coFeedbackTransformation.addFeedbackEdge(feedbackStream.getTransformation());

            return feedbackStream;
        }

        private UnsupportedOperationException groupingException =
                new UnsupportedOperationException(
                        "Cannot change the input partitioning of an"
                                + "iteration head directly. Apply the partitioning on the input and"
                                + "feedback streams instead.");

        @Override
        public ConnectedStreams<I, F> keyBy(int[] keyPositions1, int[] keyPositions2) {
            throw groupingException;
        }

        @Override
        public ConnectedStreams<I, F> keyBy(String field1, String field2) {
            throw groupingException;
        }

        @Override
        public ConnectedStreams<I, F> keyBy(String[] fields1, String[] fields2) {
            throw groupingException;
        }

        @Override
        public <KEY> ConnectedStreams<I, F> keyBy(
                KeySelector<I, KEY> keySelector1, KeySelector<F, KEY> keySelector2) {
            throw groupingException;
        }

        @Override
        public <KEY> ConnectedStreams<I, F> keyBy(
                KeySelector<I, KEY> keySelector1,
                KeySelector<F, KEY> keySelector2,
                TypeInformation<KEY> keyType) {
            throw groupingException;
        }
    }
}
