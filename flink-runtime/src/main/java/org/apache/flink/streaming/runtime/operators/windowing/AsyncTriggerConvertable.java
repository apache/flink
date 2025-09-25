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

package org.apache.flink.streaming.runtime.operators.windowing;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.runtime.asyncprocessing.operators.windowing.triggers.AsyncTrigger;
import org.apache.flink.streaming.api.windowing.windows.Window;

import javax.annotation.Nonnull;

/**
 * Interface for declaring the ability to convert a sync {@code Trigger} to {@code AsyncTrigger}.
 * The trigger conversion happens in {@code AsyncTriggerConverter}.
 */
@Experimental
public interface AsyncTriggerConvertable<T, W extends Window> {
    /**
     * Convert to an {@code AsyncTrigger}.
     *
     * @return The {@code AsyncTrigger} for async state processing.
     */
    @Nonnull
    AsyncTrigger<T, W> convertToAsync();
}
