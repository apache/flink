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

package org.apache.flink.streaming.util.watermark;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.watermark.Watermark;
import org.apache.flink.api.common.watermark.WatermarkDeclaration;
import org.apache.flink.datastream.api.extension.eventtime.EventTimeExtension;
import org.apache.flink.datastream.api.function.ProcessFunction;
import org.apache.flink.runtime.asyncprocessing.operators.AbstractAsyncStateUdfStreamOperator;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.SourceOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.runtime.watermark.AbstractInternalWatermarkDeclaration;
import org.apache.flink.streaming.runtime.watermark.WatermarkCombiner;
import org.apache.flink.streaming.runtime.watermark.extension.eventtime.EventTimeWatermarkCombiner;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** Utils class for {@link Watermark}. */
public final class WatermarkUtils {

    /**
     * Retrieve the declared watermarks from StreamGraph and convert them into {@code
     * InternalWatermarkDeclaration}.
     */
    public static Set<AbstractInternalWatermarkDeclaration<?>>
            getInternalWatermarkDeclarationsFromStreamGraph(StreamGraph streamGraph) {
        Collection<StreamNode> streamNodes = streamGraph.getStreamNodes();
        Set<WatermarkDeclaration> declarations =
                streamNodes.stream()
                        .map(StreamNode::getOperatorFactory)
                        .filter(
                                factory ->
                                        factory instanceof SimpleOperatorFactory
                                                || factory instanceof SourceOperatorFactory)
                        .map(
                                factory -> {
                                    if (factory instanceof SimpleOperatorFactory) {
                                        return getWatermarkDeclarations(
                                                ((SimpleOperatorFactory<?>) factory).getOperator());
                                    } else {
                                        return ((SourceOperatorFactory<?>) factory)
                                                .getSourceWatermarkDeclarations();
                                    }
                                })
                        .flatMap(Collection::stream)
                        .collect(Collectors.toSet());
        return convertToInternalWatermarkDeclarations(declarations);
    }

    /**
     * Retrieve the user-defined {@link WatermarkDeclaration}s of {@link ProcessFunction}. The
     * {@link WatermarkDeclaration} defined by the source operator can be retrieved from {@link
     * SourceOperatorFactory#getSourceWatermarkDeclarations()}.
     */
    private static Collection<? extends WatermarkDeclaration> getWatermarkDeclarations(
            StreamOperator<?> streamOperator) {
        if (streamOperator instanceof AbstractAsyncStateUdfStreamOperator) {
            Function f =
                    ((AbstractAsyncStateUdfStreamOperator<?, ?>) streamOperator).getUserFunction();
            if (f instanceof ProcessFunction) {
                return ((ProcessFunction) f).declareWatermarks();
            }
        }
        return Collections.emptySet();
    }

    /**
     * Convert user-oriented {@link WatermarkDeclaration} instance to internal-oriented {@link
     * AbstractInternalWatermarkDeclaration} instance.
     */
    public static Set<AbstractInternalWatermarkDeclaration<?>>
            convertToInternalWatermarkDeclarations(
                    Set<WatermarkDeclaration> watermarkDeclarations) {
        return watermarkDeclarations.stream()
                .map(AbstractInternalWatermarkDeclaration::from)
                .collect(Collectors.toSet());
    }

    /** Create watermark combiners if there are event time watermark declarations. */
    public static void addEventTimeWatermarkCombinerIfNeeded(
            Set<AbstractInternalWatermarkDeclaration<?>> watermarkDeclarationSet,
            Map<String, WatermarkCombiner> watermarkCombiners,
            int numberOfInputChannels) {
        if (watermarkDeclarationSet.stream()
                .anyMatch(
                        declaration ->
                                EventTimeExtension.isEventTimeWatermark(
                                        declaration.getIdentifier()))) {
            // create event time watermark combiner
            EventTimeWatermarkCombiner eventTimeWatermarkCombiner =
                    new EventTimeWatermarkCombiner(numberOfInputChannels);
            watermarkCombiners.put(
                    EventTimeExtension.EVENT_TIME_WATERMARK_DECLARATION.getIdentifier(),
                    eventTimeWatermarkCombiner);
            watermarkCombiners.put(
                    EventTimeExtension.IDLE_STATUS_WATERMARK_DECLARATION.getIdentifier(),
                    eventTimeWatermarkCombiner);
        }
    }
}
