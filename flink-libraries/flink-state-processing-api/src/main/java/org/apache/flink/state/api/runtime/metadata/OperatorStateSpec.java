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

package org.apache.flink.state.api.runtime.metadata;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.state.api.runtime.BootstrapTransformationWithID;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

/**
 * This class specifies an operator state maintained by {@link SavepointMetadata}. An operator state
 * is either represented as an existing {@link OperatorState}, or a {@link
 * org.apache.flink.state.api.BootstrapTransformation} that will be used to create it.
 */
@Internal
class OperatorStateSpec {

    private final OperatorID id;

    @Nullable private final OperatorState existingState;

    @Nullable private final BootstrapTransformationWithID<?> newOperatorStateTransformation;

    static OperatorStateSpec existing(OperatorState existingState) {
        return new OperatorStateSpec(Preconditions.checkNotNull(existingState));
    }

    static OperatorStateSpec newWithTransformation(
            BootstrapTransformationWithID<?> transformation) {
        return new OperatorStateSpec(Preconditions.checkNotNull(transformation));
    }

    private OperatorStateSpec(OperatorState existingState) {
        this.id = existingState.getOperatorID();
        this.existingState = existingState;
        this.newOperatorStateTransformation = null;
    }

    private OperatorStateSpec(BootstrapTransformationWithID<?> transformation) {
        this.id = transformation.getOperatorID();
        this.newOperatorStateTransformation = transformation;
        this.existingState = null;
    }

    boolean isExistingState() {
        return existingState != null;
    }

    boolean isNewStateTransformation() {
        return !isExistingState();
    }

    OperatorState asExistingState() {
        Preconditions.checkState(
                isExistingState(), "OperatorState %s is not an existing state.", id);
        return existingState;
    }

    @SuppressWarnings("unchecked")
    <T> BootstrapTransformationWithID<T> asNewStateTransformation() {
        Preconditions.checkState(
                isNewStateTransformation(),
                "OperatorState %s is not a new state defined with BootstrapTransformation",
                id);
        return (BootstrapTransformationWithID<T>) newOperatorStateTransformation;
    }
}
