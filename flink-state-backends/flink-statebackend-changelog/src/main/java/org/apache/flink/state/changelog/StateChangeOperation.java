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

package org.apache.flink.state.changelog;

import org.apache.flink.annotation.Internal;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** The operation applied to {@link ChangelogState}. */
@Internal
public enum StateChangeOperation {
    /** Scope: key + namespace. */
    CLEAR((byte) 0),
    /** Scope: key + namespace. */
    SET((byte) 1),
    /** Scope: key + namespace. */
    SET_INTERNAL((byte) 2),
    /** Scope: key + namespace. */
    ADD((byte) 3),
    /** Scope: key + namespace, also affecting other (source) namespaces. */
    MERGE_NS((byte) 4),
    /** Scope: key + namespace + element (e.g. user list append). */
    ADD_ELEMENT((byte) 5),
    /** Scope: key + namespace + element (e.g. user map key put). */
    ADD_OR_UPDATE_ELEMENT((byte) 6),
    /** Scope: key + namespace + element (e.g. user map remove or iterator remove). */
    REMOVE_ELEMENT((byte) 7),
    /** Scope: key + namespace, first element (e.g. priority queue poll). */
    REMOVE_FIRST_ELEMENT((byte) 8),
    /** State metadata (name, serializers, etc.). */
    METADATA((byte) 9);
    private final byte code;

    StateChangeOperation(byte code) {
        this.code = code;
    }

    private static final Map<Byte, StateChangeOperation> BY_CODES =
            Arrays.stream(StateChangeOperation.values())
                    .collect(Collectors.toMap(o -> o.code, Function.identity()));

    public static StateChangeOperation byCode(byte opCode) {
        return checkNotNull(BY_CODES.get(opCode), Byte.toString(opCode));
    }

    public byte getCode() {
        return code;
    }
}
