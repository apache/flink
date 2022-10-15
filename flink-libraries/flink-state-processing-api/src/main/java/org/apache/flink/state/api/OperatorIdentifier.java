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

package org.apache.flink.state.api;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.state.api.runtime.OperatorIDGenerator;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Objects;
import java.util.Optional;

/** Identifies an operator, either based on a {@code uid} or {@code uidHash}. */
@PublicEvolving
public final class OperatorIdentifier implements Serializable {
    // this is only used for logging purposes
    @Nullable private final String uid;
    // this is the runtime representation of a uid hash
    private final OperatorID operatorId;

    private OperatorIdentifier(OperatorID operatorId, @Nullable String uid) {
        this.operatorId = operatorId;
        this.uid = uid;
    }

    public static OperatorIdentifier forUidHash(String uidHash) {
        Preconditions.checkNotNull(uidHash);
        return new OperatorIdentifier(new OperatorID(StringUtils.hexStringToByte(uidHash)), null);
    }

    public static OperatorIdentifier forUid(String uid) {
        Preconditions.checkNotNull(uid);
        return new OperatorIdentifier(OperatorIDGenerator.fromUid(uid), uid);
    }

    @Internal
    public Optional<String> getUid() {
        return Optional.ofNullable(uid);
    }

    @Internal
    public OperatorID getOperatorId() {
        return operatorId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        OperatorIdentifier that = (OperatorIdentifier) o;
        return Objects.equals(operatorId, that.operatorId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(operatorId);
    }

    @Override
    public String toString() {
        return uid != null ? uid + "(" + operatorId.toHexString() + ")" : operatorId.toHexString();
    }
}
