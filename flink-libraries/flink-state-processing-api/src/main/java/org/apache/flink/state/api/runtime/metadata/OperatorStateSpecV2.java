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
import org.apache.flink.state.api.OperatorIdentifier;
import org.apache.flink.state.api.runtime.StateBootstrapTransformationWithID;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

/**
 * This class specifies an operator state maintained by {@link SavepointMetadataV2}. An operator
 * state is either represented as an existing {@link OperatorState}, or a {@link
 * org.apache.flink.state.api.StateBootstrapTransformation} that will be used to create it.
 */
@Internal
class OperatorStateSpecV2 {

    private final OperatorIdentifier identifier;

    @Nullable private final OperatorState existingState;

    @Nullable private final StateBootstrapTransformationWithID<?> newOperatorStateTransformation;

    static OperatorStateSpecV2 existing(OperatorState existingState) {
        return new OperatorStateSpecV2(Preconditions.checkNotNull(existingState));
    }

    static OperatorStateSpecV2 newWithTransformation(
            StateBootstrapTransformationWithID<?> transformation) {
        return new OperatorStateSpecV2(Preconditions.checkNotNull(transformation));
    }

    private OperatorStateSpecV2(OperatorState existingState) {
        if (existingState.getOperatorUid().isPresent()) {
            this.identifier = OperatorIdentifier.forUid(existingState.getOperatorUid().get());
        } else {
            this.identifier =
                    OperatorIdentifier.forUidHash(existingState.getOperatorID().toHexString());
        }
        this.existingState = existingState;
        this.newOperatorStateTransformation = null;
    }

    private OperatorStateSpecV2(StateBootstrapTransformationWithID<?> transformation) {
        this.identifier = transformation.getOperatorIdentifier();
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
                isExistingState(), "OperatorState %s is not an existing state.", identifier);
        return existingState;
    }

    @SuppressWarnings("unchecked")
    <T> StateBootstrapTransformationWithID<T> asNewStateTransformation() {
        Preconditions.checkState(
                isNewStateTransformation(),
                "OperatorState %s is not a new state defined with BootstrapTransformation",
                identifier);
        return (StateBootstrapTransformationWithID<T>) newOperatorStateTransformation;
    }
}
