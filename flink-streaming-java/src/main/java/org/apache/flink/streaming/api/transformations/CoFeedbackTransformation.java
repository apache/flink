/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.transformations;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Transformation;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import java.util.Collections;
import java.util.List;

/**
 * This represents a feedback point in a topology. The type of the feedback elements need not match
 * the type of the upstream {@code Transformation} because the only allowed operations after a
 * {@code CoFeedbackTransformation} are {@link
 * org.apache.flink.streaming.api.transformations.TwoInputTransformation TwoInputTransformations}.
 * The upstream {@code Transformation} will be connected to the first input of the Co-Transform
 * while the feedback edges will be connected to the second input.
 *
 * <p>Both the partitioning of the input and the feedback edges is preserved. They can also have
 * differing partitioning strategies. This requires, however, that the parallelism of the feedback
 * {@code Transformations} must match the parallelism of the input {@code Transformation}.
 *
 * <p>The upstream {@code Transformation} is not wired to this {@code CoFeedbackTransformation}. It
 * is instead directly wired to the {@code TwoInputTransformation} after this {@code
 * CoFeedbackTransformation}.
 *
 * <p>This is different from Iterations in batch processing.
 *
 * @see org.apache.flink.streaming.api.transformations.FeedbackTransformation
 * @param <F> The type of the feedback elements.
 */
@Internal
public class CoFeedbackTransformation<F> extends Transformation<F> {

    private final List<Transformation<F>> feedbackEdges;

    private final Long waitTime;

    /**
     * Creates a new {@code CoFeedbackTransformation} from the given input.
     *
     * @param parallelism The parallelism of the upstream {@code Transformation} and the feedback
     *     edges.
     * @param feedbackType The type of the feedback edges
     * @param waitTime The wait time of the feedback operator. After the time expires the operation
     *     will close and not receive any more feedback elements.
     */
    public CoFeedbackTransformation(
            int parallelism, TypeInformation<F> feedbackType, Long waitTime) {
        super("CoFeedback", feedbackType, parallelism);
        this.waitTime = waitTime;
        this.feedbackEdges = Lists.newArrayList();
    }

    /**
     * Adds a feedback edge. The parallelism of the {@code Transformation} must match the
     * parallelism of the input {@code Transformation} of the upstream {@code Transformation}.
     *
     * @param transform The new feedback {@code Transformation}.
     */
    public void addFeedbackEdge(Transformation<F> transform) {

        if (transform.getParallelism() != this.getParallelism()) {
            throw new UnsupportedOperationException(
                    "Parallelism of the feedback stream must match the parallelism of the original"
                            + " stream. Parallelism of original stream: "
                            + this.getParallelism()
                            + "; parallelism of feedback stream: "
                            + transform.getParallelism());
        }

        feedbackEdges.add(transform);
    }

    /** Returns the list of feedback {@code Transformations}. */
    public List<Transformation<F>> getFeedbackEdges() {
        return feedbackEdges;
    }

    /**
     * Returns the wait time. This is the amount of time that the feedback operator keeps listening
     * for feedback elements. Once the time expires the operation will close and will not receive
     * further elements.
     */
    public Long getWaitTime() {
        return waitTime;
    }

    @Override
    public List<Transformation<?>> getTransitivePredecessors() {
        return Collections.singletonList(this);
    }

    @Override
    public List<Transformation<?>> getInputs() {
        return Collections.emptyList();
    }
}
