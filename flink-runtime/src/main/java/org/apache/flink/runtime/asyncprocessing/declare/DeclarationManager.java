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

package org.apache.flink.runtime.asyncprocessing.declare;

import java.util.HashMap;
import java.util.Map;

/** The manager holds all the declaration information and manage the building procedure. */
public class DeclarationManager {

    private final Map<String, NamedCallback> knownCallbacks;

    private int nextValidNameSequence = 0;

    public DeclarationManager() {
        this.knownCallbacks = new HashMap<>();
    }

    <T extends NamedCallback> T register(T knownCallback) throws DeclarationException {
        if (knownCallbacks.put(knownCallback.getName(), knownCallback) != null) {
            throw new DeclarationException("Duplicated key " + knownCallback.getName());
        }
        return knownCallback;
    }

    String nextAssignedName() {
        String name;
        do {
            name = String.format("___%d___", nextValidNameSequence++);
        } while (knownCallbacks.containsKey(name));
        return name;
    }
}
