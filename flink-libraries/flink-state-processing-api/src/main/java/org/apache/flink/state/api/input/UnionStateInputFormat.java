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

package org.apache.flink.state.api.input;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.util.Preconditions;

/**
 * Input format for reading operator union state.
 *
 * @param <OT> The generic type of the state
 */
@Internal
public class UnionStateInputFormat<OT> extends OperatorStateInputFormat<OT> {

    private static final long serialVersionUID = 499388405631162440L;

    private final ListStateDescriptor<OT> descriptor;

    /**
     * Creates an input format for reading union state from an operator in a savepoint.
     *
     * @param operatorState The state to be queried.
     * @param descriptor The descriptor for this state, providing a name and serializer.
     */
    public UnionStateInputFormat(OperatorState operatorState, ListStateDescriptor<OT> descriptor) {
        super(operatorState, true);

        this.descriptor =
                Preconditions.checkNotNull(descriptor, "The state descriptor must not be null");
    }

    @Override
    protected final Iterable<OT> getElements(OperatorStateBackend restoredBackend)
            throws Exception {
        return restoredBackend.getUnionListState(descriptor).get();
    }
}
