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

package org.apache.flink.runtime.io.network.api.writer;

import org.apache.flink.runtime.io.network.partition.MockResultPartitionWriter;

import java.util.concurrent.CompletableFuture;

/**
 * A specific result partition writer implementation only used to control the output availability
 * state in tests.
 */
public class ResultPartitionWriterWithAvailabilityHelper extends MockResultPartitionWriter {

    private final AvailabilityHelper availabilityHelper;

    public ResultPartitionWriterWithAvailabilityHelper(AvailabilityHelper availabilityHelper) {
        this.availabilityHelper = availabilityHelper;
    }

    @Override
    public CompletableFuture<?> getAvailableFuture() {
        return availabilityHelper.getAvailableFuture();
    }

    @Override
    public boolean isAvailable() {
        return availabilityHelper.isAvailable();
    }
}
