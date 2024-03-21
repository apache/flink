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

package org.apache.flink.process.impl.context;

import org.apache.flink.process.api.context.TimestampManager;

import java.util.Optional;

/** The default implementation of {@link TimestampManager}. */
public class DefaultTimestampManager implements TimestampManager {
    private Optional<Long> currentTimestamp;

    @Override
    public Optional<Long> getCurrentRecordTimestamp() {
        return currentTimestamp;
    }

    public void setTimestamp(Optional<Long> timestamp) {
        currentTimestamp = timestamp;
    }

    public void resetTimestamp() {
        currentTimestamp = Optional.empty();
    }
}
