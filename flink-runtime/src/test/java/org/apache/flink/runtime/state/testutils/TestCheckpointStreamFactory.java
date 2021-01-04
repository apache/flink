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

package org.apache.flink.runtime.state.testutils;

import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointedStateScope;

import java.util.function.Supplier;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A bridge between factory lambdas (like {@link Supplier} and the CheckpointStreamFactory class.
 */
public class TestCheckpointStreamFactory implements CheckpointStreamFactory {

    private final Supplier<CheckpointStateOutputStream> supplier;

    public TestCheckpointStreamFactory(Supplier<CheckpointStateOutputStream> supplier) {
        this.supplier = checkNotNull(supplier);
    }

    @Override
    public CheckpointStateOutputStream createCheckpointStateOutputStream(
            CheckpointedStateScope scope) {
        return supplier.get();
    }
}
