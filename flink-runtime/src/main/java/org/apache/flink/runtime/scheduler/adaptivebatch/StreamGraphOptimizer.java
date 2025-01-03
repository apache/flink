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

package org.apache.flink.runtime.scheduler.adaptivebatch;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.graph.StreamGraphContext;
import org.apache.flink.util.DynamicCodeLoadingException;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The {@code StreamGraphOptimizer} class is responsible for optimizing a StreamGraph based on
 * runtime information.
 *
 * <p>Upon initialization, it obtains a {@code StreamGraphContext} from the {@code
 * AdaptiveGraphManager} and loads the specified optimization strategies. At runtime, it applies
 * these strategies sequentially to the StreamGraph using the provided context and information about
 * finished operators.
 */
public class StreamGraphOptimizer {

    private final List<StreamGraphOptimizationStrategy> optimizationStrategies;

    public StreamGraphOptimizer(Configuration jobConfiguration, ClassLoader userClassLoader)
            throws DynamicCodeLoadingException {
        checkNotNull(jobConfiguration);

        Optional<List<String>> optional =
                jobConfiguration.getOptional(
                        StreamGraphOptimizationStrategy.STREAM_GRAPH_OPTIMIZATION_STRATEGY);
        if (optional.isPresent()) {
            optimizationStrategies = loadOptimizationStrategies(optional.get(), userClassLoader);
        } else {
            optimizationStrategies = new ArrayList<>();
        }
    }

    public void initializeStrategies(StreamGraphContext context) {
        checkNotNull(optimizationStrategies).forEach(strategy -> strategy.initialize(context));
    }

    /**
     * Applies all loaded optimization strategies to the StreamGraph.
     *
     * @param operatorsFinished the object containing information about finished operators.
     * @param context the StreamGraphContext providing methods to modify the StreamGraph.
     */
    public void onOperatorsFinished(OperatorsFinished operatorsFinished, StreamGraphContext context)
            throws Exception {
        for (StreamGraphOptimizationStrategy strategy : optimizationStrategies) {
            strategy.onOperatorsFinished(operatorsFinished, context);
        }
    }

    private List<StreamGraphOptimizationStrategy> loadOptimizationStrategies(
            List<String> strategyClassNames, ClassLoader userClassLoader)
            throws DynamicCodeLoadingException {
        List<StreamGraphOptimizationStrategy> strategies =
                new ArrayList<>(strategyClassNames.size());

        for (String strategyClassName : strategyClassNames) {
            strategies.add(loadOptimizationStrategy(strategyClassName, userClassLoader));
        }

        return strategies;
    }

    private StreamGraphOptimizationStrategy loadOptimizationStrategy(
            String strategyClassName, ClassLoader userClassLoader)
            throws DynamicCodeLoadingException {
        try {
            Class<? extends StreamGraphOptimizationStrategy> clazz =
                    Class.forName(strategyClassName, false, userClassLoader)
                            .asSubclass(StreamGraphOptimizationStrategy.class);

            return clazz.getDeclaredConstructor().newInstance();
        } catch (ClassNotFoundException e) {
            throw new DynamicCodeLoadingException(
                    "Cannot find configured stream graph optimization strategy class: "
                            + strategyClassName,
                    e);
        } catch (ClassCastException
                | InstantiationException
                | IllegalAccessException
                | NoSuchMethodException
                | InvocationTargetException e) {
            throw new DynamicCodeLoadingException(
                    "The configured class '"
                            + strategyClassName
                            + "' is not a valid stream graph optimization strategy",
                    e);
        }
    }
}
