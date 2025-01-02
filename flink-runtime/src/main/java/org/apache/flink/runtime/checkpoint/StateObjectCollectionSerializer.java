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

package org.apache.flink.runtime.checkpoint;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.serializers.CollectionSerializer;

import java.util.ArrayList;

// Kryo will default to using the CollectionSerializer for StateObjectCollection.
// The problem is that will instantiate new StateObjectCollection instances with null values
// which are invalid. This overrides the create method so that StateObjectCollection are
// created with normal constructors and an empty ArrayList of the right size.
public class StateObjectCollectionSerializer
        extends CollectionSerializer<StateObjectCollection<?>> {
    @Override
    protected StateObjectCollection<?> create(
            Kryo kryo, Input input, Class<? extends StateObjectCollection<?>> type, int size) {
        return new StateObjectCollection<>(new ArrayList<>(size));
    }
}
