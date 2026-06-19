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

package org.apache.flink.datastream.api.extension.eventtime.function;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.datastream.api.extension.eventtime.timer.EventTimeManager;
import org.apache.flink.datastream.api.function.ProcessFunction;
import org.apache.flink.datastream.api.stream.KeyedPartitionStream;

/**
 * The base interface for event time processing, indicating that the {@link ProcessFunction} will be
 * enriched with event time processing functions, such as registering event timers and handle event
 * time watermarks.
 *
 * <p>Note that registering event timers can only be used with {@link KeyedPartitionStream}.
 */
@Experimental
public interface EventTimeProcessFunction extends ProcessFunction {
    /**
     * Initialize the {@link EventTimeProcessFunction} with an instance of {@link EventTimeManager}.
     * Note that this method should be invoked before the open method.
     */
    void initEventTimeProcessFunction(EventTimeManager eventTimeManager);
}
