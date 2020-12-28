/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
 * Unique ID that allows for logical comparison between state handles.
 *
 * <p>Two state handles that are considered as logically equal should always return the same ID
 * (whatever logically equal means is up to the implementation). For example, this could be based on
 * the string representation of the full filepath for a state that is based on a file.
 */
public class StateHandleID extends StringBasedID {

    private static final long serialVersionUID = 1L;

    public StateHandleID(String keyString) {
        super(keyString);
    }
}
