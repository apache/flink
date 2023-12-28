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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Experimental;

/**
 * OperatorAttributes element provides Job Manager with information that can be used to optimize job
 * performance.
 */
@Experimental
public class OperatorAttributes {

    private final boolean outputOnEOF;
    private final boolean internalSorterSupported;

    public OperatorAttributes(boolean isOutputOnEOF, boolean internalSorterSupported) {
        this.outputOnEOF = isOutputOnEOF;
        this.internalSorterSupported = internalSorterSupported;
    }

    /**
     * Returns true iff the operator can only emit records after inputs have reached EOF.
     *
     * <p>Here are the implications when it is true:
     *
     * <ul>
     *   <li>The results of this operator as well as its chained operators have blocking partition
     *       type.
     *   <li>This operator as well as its chained operators will be executed in batch mode.
     * </ul>
     */
    public boolean isOutputOnEOF() {
        return outputOnEOF;
    }

    /**
     * Returns true iff the operator uses an internal sorter to sort inputs by key when any of the
     * following conditions are met:
     *
     * <ul>
     *   <li>execution.runtime-mode = BATCH.
     *   <li>execution.checkpointing.interval-during-backlog = 0 AND any of its input has
     *       isBacklog=true.
     * </ul>
     *
     * <p>Here are the implications when it is true:
     *
     * <ul>
     *   <li>Its input records will not to be sorted externally before being fed into this operator.
     *   <li>Its managed memory will be set according to execution.sorted-inputs.memory.
     * </ul>
     */
    public boolean isInternalSorterSupported() {
        return internalSorterSupported;
    }
}
