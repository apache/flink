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

package org.apache.flink.graph.gsa;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.IterationConfiguration;

import java.util.ArrayList;
import java.util.List;

/**
 * A GSAConfiguration object can be used to set the iteration name and degree of parallelism, to
 * register aggregators and use broadcast sets in the {@link
 * org.apache.flink.graph.gsa.GatherFunction}, {@link org.apache.flink.graph.gsa.SumFunction} as
 * well as {@link org.apache.flink.graph.gsa.ApplyFunction}.
 *
 * <p>The GSAConfiguration object is passed as an argument to {@link
 * org.apache.flink.graph.Graph#runGatherSumApplyIteration(org.apache.flink.graph.gsa.GatherFunction,
 * org.apache.flink.graph.gsa.SumFunction, org.apache.flink.graph.gsa.ApplyFunction, int)}
 */
public class GSAConfiguration extends IterationConfiguration {

    // the broadcast variables for the gather function
    private List<Tuple2<String, DataSet<?>>> bcVarsGather = new ArrayList<>();

    // the broadcast variables for the sum function
    private List<Tuple2<String, DataSet<?>>> bcVarsSum = new ArrayList<>();

    // the broadcast variables for the apply function
    private List<Tuple2<String, DataSet<?>>> bcVarsApply = new ArrayList<>();

    private EdgeDirection direction = EdgeDirection.OUT;

    public GSAConfiguration() {}

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
     * Adds a data set as a broadcast set to the sum function.
     *
     * @param name The name under which the broadcast data is available in the sum function.
     * @param data The data set to be broadcast.
     */
    public void addBroadcastSetForSumFunction(String name, DataSet<?> data) {
        this.bcVarsSum.add(new Tuple2<>(name, data));
    }

    /**
     * Adds a data set as a broadcast set to the apply function.
     *
     * @param name The name under which the broadcast data is available in the apply function.
     * @param data The data set to be broadcast.
     */
    public void addBroadcastSetForApplyFunction(String name, DataSet<?> data) {
        this.bcVarsApply.add(new Tuple2<>(name, data));
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
     * Get the broadcast variables of the SumFunction.
     *
     * @return a List of Tuple2, where the first field is the broadcast variable name and the second
     *     field is the broadcast DataSet.
     */
    public List<Tuple2<String, DataSet<?>>> getSumBcastVars() {
        return this.bcVarsSum;
    }

    /**
     * Get the broadcast variables of the ApplyFunction.
     *
     * @return a List of Tuple2, where the first field is the broadcast variable name and the second
     *     field is the broadcast DataSet.
     */
    public List<Tuple2<String, DataSet<?>>> getApplyBcastVars() {
        return this.bcVarsApply;
    }

    /**
     * Gets the direction from which the neighbors are to be selected By default the neighbors who
     * are target of the edges are selected.
     *
     * @return an EdgeDirection, which can be either IN, OUT or ALL.
     */
    public EdgeDirection getDirection() {
        return direction;
    }

    /**
     * Sets the direction in which neighbors are to be selected By default the neighbors who are
     * target of the edges are selected.
     *
     * @param direction - IN, OUT or ALL
     */
    public void setDirection(EdgeDirection direction) {
        this.direction = direction;
    }
}
