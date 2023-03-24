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

package org.apache.flink.streaming.api.runners.python.beam.state;

import java.util.Map;

/** The type of the Python map state iterate request. */
public enum IterateType {

    /** Equivalent to iterate {@link Map#entrySet() }. */
    ITEMS((byte) 0),

    /** Equivalent to iterate {@link Map#keySet() }. */
    KEYS((byte) 1),

    /** Equivalent to iterate {@link Map#values() }. */
    VALUES((byte) 2);

    private final byte ord;

    IterateType(byte ord) {
        this.ord = ord;
    }

    public byte getOrd() {
        return ord;
    }

    public static IterateType fromOrd(byte ord) {
        switch (ord) {
            case 0:
                return ITEMS;
            case 1:
                return KEYS;
            case 2:
                return VALUES;
            default:
                throw new RuntimeException("Unsupported ordinal: " + ord);
        }
    }
}
