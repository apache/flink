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

package org.apache.flink.table.runtime.operators.window.slicing;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.operators.InternalTimerService;

/**
 * A clock service which can get current processing time. This can help to mock processing time in
 * tests.
 */
@Internal
public interface ClockService {

    /** Returns the current processing time. */
    long currentProcessingTime();

    /** Creates a {@link ClockService} from the given {@link InternalTimerService}. */
    static ClockService of(InternalTimerService<?> timerService) {
        return timerService::currentProcessingTime;
    }
}
