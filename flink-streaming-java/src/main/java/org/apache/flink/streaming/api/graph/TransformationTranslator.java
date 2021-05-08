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

package org.apache.flink.streaming.api.graph;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;

import java.util.Collection;

/**
 * A {@code TransformationTranslator} is responsible for translating a given {@link Transformation}
 * to its runtime implementation depending on the execution mode.
 *
 * @param <OUT> The type of the output elements of the transformation being translated.
 * @param <T> The type of transformation being translated.
 */
@Internal
public interface TransformationTranslator<OUT, T extends Transformation<OUT>> {

    /**
     * Translates a given {@link Transformation} to its runtime implementation for BATCH-style
     * execution.
     *
     * @param transformation The transformation to be translated.
     * @param context The translation context.
     * @return The ids of the "last" {@link StreamNode StreamNodes} in the transformation graph
     *     corresponding to this transformation. These will be the nodes that a potential following
     *     transformation will need to connect to.
     */
    Collection<Integer> translateForBatch(final T transformation, final Context context);

    /**
     * Translates a given {@link Transformation} to its runtime implementation for STREAMING-style
     * execution.
     *
     * @param transformation The transformation to be translated.
     * @param context The translation context.
     * @return The ids of the "last" {@link StreamNode StreamNodes} in the transformation graph
     *     corresponding to this transformation. These will be the nodes that a potential following
     *     transformation will need to connect to.
     */
    Collection<Integer> translateForStreaming(final T transformation, final Context context);

    /** A context giving the necessary information for the translation of a given transformation. */
    interface Context {

        /**
         * Returns the {@link StreamGraph} being created as the transformations of a pipeline are
         * translated to their runtime implementations.
         */
        StreamGraph getStreamGraph();

        /**
         * Returns the ids of the nodes in the {@link StreamGraph} corresponding to the provided
         * transformation.
         *
         * @param transformation the transformation whose nodes' ids we want.
         * @return The requested ids.
         */
        Collection<Integer> getStreamNodeIds(final Transformation<?> transformation);

        /** Returns the slot sharing group for the given transformation. */
        String getSlotSharingGroup();

        /** Returns the default buffer timeout to be used. */
        long getDefaultBufferTimeout();

        /** Retrieves additional configuration for the graph generation process. */
        ReadableConfig getGraphGeneratorConfig();
    }
}
