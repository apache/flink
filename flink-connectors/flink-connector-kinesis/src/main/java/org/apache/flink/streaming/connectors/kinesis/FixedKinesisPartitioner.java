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

package org.apache.flink.streaming.connectors.kinesis;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.Preconditions;

import java.util.Objects;

/**
 * A partitioner ensuring that each internal Flink partition ends up in the same Kinesis partition.
 *
 * <p>This is achieved by using the index of the producer task as a {@code PartitionKey}.
 */
@PublicEvolving
public final class FixedKinesisPartitioner<T> extends KinesisPartitioner<T> {

    private static final long serialVersionUID = 1L;

    private int indexOfThisSubtask = 0;

    @Override
    public void initialize(int indexOfThisSubtask, int numberOfParallelSubtasks) {
        Preconditions.checkArgument(
                indexOfThisSubtask >= 0, "Id of this subtask cannot be negative.");
        Preconditions.checkArgument(
                numberOfParallelSubtasks > 0, "Number of subtasks must be larger than 0.");

        this.indexOfThisSubtask = indexOfThisSubtask;
    }

    @Override
    public String getPartitionId(T record) {
        return String.valueOf(indexOfThisSubtask);
    }

    // --------------------------------------------------------------------------------------------
    // Value semantics for equals and hashCode
    // --------------------------------------------------------------------------------------------

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final FixedKinesisPartitioner<?> that = (FixedKinesisPartitioner<?>) o;
        return Objects.equals(this.indexOfThisSubtask, that.indexOfThisSubtask);
    }

    @Override
    public int hashCode() {
        return Objects.hash(FixedKinesisPartitioner.class.hashCode(), indexOfThisSubtask);
    }
}
