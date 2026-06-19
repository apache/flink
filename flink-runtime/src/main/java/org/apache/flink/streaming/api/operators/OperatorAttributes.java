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

import java.io.Serializable;

/**
 * OperatorAttributes element provides Job Manager with information that can be used to optimize job
 * performance.
 */
@Experimental
public class OperatorAttributes implements Serializable {
    private static final long serialVersionUID = 1L;

    private final boolean outputOnlyAfterEndOfStream;
    private final boolean internalSorterSupported;

    OperatorAttributes(boolean outputOnlyAfterEndOfStream, boolean internalSorterSupported) {
        this.outputOnlyAfterEndOfStream = outputOnlyAfterEndOfStream;
        this.internalSorterSupported = internalSorterSupported;
    }

    /**
     * Returns true if and only if the operator only emits records after all its inputs have ended.
     *
     * <p>Here are the implications when it is true:
     *
     * <ul>
     *   <li>The results of this operator as well as its chained operators have blocking partition
     *       type.
     *   <li>This operator as well as its chained operators will be executed in batch mode.
     * </ul>
     */
    public boolean isOutputOnlyAfterEndOfStream() {
        return outputOnlyAfterEndOfStream;
    }

    /**
     * Returns true iff the operator uses an internal sorter to sort inputs by key. When it is true,
     * the input records will not to be sorted externally before being fed into this operator.
     */
    public boolean isInternalSorterSupported() {
        return internalSorterSupported;
    }
}
