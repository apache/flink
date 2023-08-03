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

package org.apache.flink.api.java.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.operators.Keys;
import org.apache.flink.api.java.DataSet;

/**
 * Grouping is an intermediate step for a transformation on a grouped DataSet.
 *
 * <p>The following transformation can be applied on Grouping:
 *
 * <ul>
 *   <li>{@link UnsortedGrouping#reduce(org.apache.flink.api.common.functions.ReduceFunction)},
 *   <li>{@link
 *       UnsortedGrouping#reduceGroup(org.apache.flink.api.common.functions.GroupReduceFunction)},
 *       and
 *   <li>{@link UnsortedGrouping#aggregate(org.apache.flink.api.java.aggregation.Aggregations,
 *       int)}.
 * </ul>
 *
 * @param <T> The type of the elements of the grouped DataSet.
 * @see DataSet
 * @deprecated All Flink DataSet APIs are deprecated since Flink 1.18 and will be removed in a
 *     future Flink major version. You can still build your application in DataSet, but you should
 *     move to either the DataStream and/or Table API.
 * @see <a href="https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=158866741">
 *     FLIP-131: Consolidate the user-facing Dataflow SDKs/APIs (and deprecate the DataSet API</a>
 */
@Deprecated
@Public
public abstract class Grouping<T> {

    protected final DataSet<T> inputDataSet;

    protected final Keys<T> keys;

    protected Partitioner<?> customPartitioner;

    public Grouping(DataSet<T> set, Keys<T> keys) {
        if (set == null || keys == null) {
            throw new NullPointerException();
        }

        if (keys.isEmpty()) {
            throw new InvalidProgramException("The grouping keys must not be empty.");
        }

        this.inputDataSet = set;
        this.keys = keys;
    }

    /**
     * Returns the input DataSet of a grouping operation, that is the one before the grouping. This
     * means that if it is applied directly to the result of a grouping operation, it will cancel
     * its effect. As an example, in the following snippet:
     *
     * <pre>{@code
     * DataSet<X> notGrouped = input.groupBy().getDataSet();
     * DataSet<Y> allReduced = notGrouped.reduce()
     * }</pre>
     *
     * <p>the {@code groupBy()} is as if it never happened, as the {@code notGrouped} DataSet
     * corresponds to the input of the {@code groupBy()} (because of the {@code getDataset()}).
     */
    @Internal
    public DataSet<T> getInputDataSet() {
        return this.inputDataSet;
    }

    @Internal
    public Keys<T> getKeys() {
        return this.keys;
    }

    /**
     * Gets the custom partitioner to be used for this grouping, or {@code null}, if none was
     * defined.
     *
     * @return The custom partitioner to be used for this grouping.
     */
    @Internal
    public Partitioner<?> getCustomPartitioner() {
        return this.customPartitioner;
    }
}
