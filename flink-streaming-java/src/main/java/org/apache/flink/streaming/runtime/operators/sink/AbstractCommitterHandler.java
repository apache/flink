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

package org.apache.flink.streaming.runtime.operators.sink;

import org.apache.flink.util.function.SupplierWithException;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;

abstract class AbstractCommitterHandler<InputT, OutputT>
        implements CommitterHandler<InputT, OutputT> {

    /** Record all the committables until commit. */
    private final Deque<InputT> committables = new ArrayDeque<>();

    @Override
    public List<OutputT> processCommittables(
            SupplierWithException<List<InputT>, Exception> committableSupplier) throws Exception {
        this.committables.addAll(committableSupplier.get());
        return Collections.emptyList();
    }

    protected List<InputT> pollCommittables() {
        List<InputT> committables = new ArrayList<>(this.committables);
        this.committables.clear();
        return committables;
    }

    @Override
    public void close() throws Exception {
        committables.clear();
    }
}
