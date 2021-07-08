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

package org.apache.flink.runtime.broadcast;

import org.apache.flink.api.common.functions.BroadcastVariableInitializer;

import java.util.ArrayList;
import java.util.List;

/**
 * The default {@link BroadcastVariableInitializer} implementation that initializes the broadcast
 * variable into a list.
 */
public class DefaultBroadcastVariableInitializer<T>
        implements BroadcastVariableInitializer<T, List<T>> {

    @Override
    public List<T> initializeBroadcastVariable(Iterable<T> data) {
        ArrayList<T> list = new ArrayList<T>();

        for (T value : data) {
            list.add(value);
        }
        return list;
    }

    // --------------------------------------------------------------------------------------------

    private static final DefaultBroadcastVariableInitializer<Object> INSTANCE =
            new DefaultBroadcastVariableInitializer<Object>();

    @SuppressWarnings("unchecked")
    public static <E> DefaultBroadcastVariableInitializer<E> instance() {
        return (DefaultBroadcastVariableInitializer<E>) INSTANCE;
    }

    private DefaultBroadcastVariableInitializer() {}
}
