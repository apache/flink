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

package org.apache.flink.graph.spargel;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.IterationConfiguration;

import java.util.ArrayList;
import java.util.List;

/**
 * A ScatterGatherConfiguration object can be used to set the iteration name and degree of
 * parallelism, to register aggregators and use broadcast sets in the {@link GatherFunction} and
 * {@link ScatterFunction}
 *
 * <p>The VertexCentricConfiguration object is passed as an argument to {@link
 * org.apache.flink.graph.Graph#runScatterGatherIteration (
 * org.apache.flink.graph.spargel.GatherFunction, org.apache.flink.graph.spargel.ScatterFunction,
 * int, ScatterGatherConfiguration)}.
 */
public class ScatterGatherConfiguration extends IterationConfiguration {

    // the broadcast variables for the scatter function
    private List<Tuple2<String, DataSet<?>>> bcVarsScatter = new ArrayList<>();

    // the broadcast variables for the gather function
    private List<Tuple2<String, DataSet<?>>> bcVarsGather = new ArrayList<>();

    // flag that defines whether the degrees option is set
    private boolean optDegrees = false;

    // the direction in which the messages should be sent
    private EdgeDirection direction = EdgeDirection.OUT;

    public ScatterGatherConfiguration() {}

    /**
     * Adds a data set as a broadcast set to the scatter function.
     *
     * @param name The name under which the broadcast data is available in the scatter function.
     * @param data The data set to be broadcast.
     */
    public void addBroadcastSetForScatterFunction(String name, DataSet<?> data) {
        this.bcVarsScatter.add(new Tuple2<>(name, data));
    }

    /**
     * Adds a data set as a broadcast set to the gather function.
     *
     * @param name The name under which the broadcast data is available in the gather function.
     * @param data The data set to be broadcast.
     */
    public void addBroadcastSetForGatherFunction(String name, DataSet<?> data) {
        this.bcVarsGather.add(new Tuple2<>(name, data));
    }

    /**
     * Get the broadcast variables of the ScatterFunction.
     *
     * @return a List of Tuple2, where the first field is the broadcast variable name and the second
     *     field is the broadcast DataSet.
     */
    public List<Tuple2<String, DataSet<?>>> getScatterBcastVars() {
        return this.bcVarsScatter;
    }

    /**
     * Get the broadcast variables of the GatherFunction.
     *
     * @return a List of Tuple2, where the first field is the broadcast variable name and the second
     *     field is the broadcast DataSet.
     */
    public List<Tuple2<String, DataSet<?>>> getGatherBcastVars() {
        return this.bcVarsGather;
    }

    /**
     * Gets whether the degrees option is set. By default, the degrees option is not set.
     *
     * @return True, if the degree option is set, false otherwise.
     */
    public boolean isOptDegrees() {
        return optDegrees;
    }

    /**
     * Sets the degree option. By default, the degrees option is not set.
     *
     * @param optDegrees True, to set this option, false otherwise.
     */
    public void setOptDegrees(boolean optDegrees) {
        this.optDegrees = optDegrees;
    }

    /**
     * Gets the direction in which messages are sent in the ScatterFunction. By default the
     * messaging direction is OUT.
     *
     * @return an EdgeDirection, which can be either IN, OUT or ALL.
     */
    public EdgeDirection getDirection() {
        return direction;
    }

    /**
     * Sets the direction in which messages are sent in the ScatterFunction. By default the
     * messaging direction is OUT.
     *
     * @param direction - IN, OUT or ALL
     */
    public void setDirection(EdgeDirection direction) {
        this.direction = direction;
    }
}
