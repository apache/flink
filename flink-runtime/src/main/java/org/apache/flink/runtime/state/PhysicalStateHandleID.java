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

package org.apache.flink.runtime.state;

import org.apache.flink.util.StringBasedID;

/**
 * Unique ID that allows for physical comparison between state handles.
 *
 * <p>Different state objects (e.g. different files) representing the same piece of data must have
 * different IDs (e.g. file names). This is different from {@link
 * org.apache.flink.runtime.state.KeyedStateHandle#getStateHandleId} which returns the same ID.
 *
 * @see StateHandleID
 */
public class PhysicalStateHandleID extends StringBasedID {

    private static final long serialVersionUID = 1L;

    public PhysicalStateHandleID(String keyString) {
        super(keyString);
    }
}
