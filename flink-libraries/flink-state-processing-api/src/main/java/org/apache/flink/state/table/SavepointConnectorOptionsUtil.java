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

package org.apache.flink.state.table;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.state.api.OperatorIdentifier;

import java.util.Optional;

import static org.apache.flink.state.table.SavepointConnectorOptions.OPERATOR_UID;
import static org.apache.flink.state.table.SavepointConnectorOptions.OPERATOR_UID_HASH;

/** Utilities for {@link SavepointConnectorOptions}. */
@PublicEvolving
public class SavepointConnectorOptionsUtil {

    public static OperatorIdentifier getOperatorIdentifier(ReadableConfig options) {
        final Optional<String> operatorUid = options.getOptional(OPERATOR_UID);
        final Optional<String> operatorUidHash = options.getOptional(OPERATOR_UID_HASH);

        if (operatorUid.isPresent() == operatorUidHash.isPresent()) {
            throw new IllegalArgumentException(
                    "Either operator uid or operator uid hash must be specified.");
        }

        return operatorUid
                .map(OperatorIdentifier::forUid)
                .orElseGet(() -> OperatorIdentifier.forUidHash(operatorUidHash.get()));
    }

    private SavepointConnectorOptionsUtil() {}
}
