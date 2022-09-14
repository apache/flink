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

package org.apache.flink.runtime.dispatcher;

import org.apache.flink.util.AbstractID;

import java.util.UUID;

/** Fencing token of the {@link Dispatcher}. */
public class DispatcherId extends AbstractID {

    private static final long serialVersionUID = -1654056277003743966L;

    private DispatcherId() {}

    private DispatcherId(UUID uuid) {
        super(uuid.getLeastSignificantBits(), uuid.getMostSignificantBits());
    }

    public UUID toUUID() {
        return new UUID(getUpperPart(), getLowerPart());
    }

    /** Generates a new random DispatcherId. */
    public static DispatcherId generate() {
        return new DispatcherId();
    }

    /** Creates a new DispatcherId that corresponds to the UUID. */
    public static DispatcherId fromUuid(UUID uuid) {
        return new DispatcherId(uuid);
    }
}
