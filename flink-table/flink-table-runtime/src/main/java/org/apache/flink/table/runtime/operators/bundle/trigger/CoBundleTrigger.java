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

package org.apache.flink.table.runtime.operators.bundle.trigger;

import org.apache.flink.annotation.Internal;

import java.io.Serializable;

/**
 * A {@link CoBundleTrigger} is similar with {@link BundleTrigger}, and the only differences is
 * {@link CoBundleTrigger} can handle two inputs.
 *
 * @param <IN1> The first input element type.
 * @param <IN2> The second input element type.
 */
@Internal
public interface CoBundleTrigger<IN1, IN2> extends Serializable {

    /** Register a callback which will be called once this trigger decides to finish this bundle. */
    void registerCallback(BundleTriggerCallback callback);

    /**
     * Called for every element that gets added to the bundle from the first input. If the trigger
     * decides to start evaluate, {@link BundleTriggerCallback#finishBundle()} should be invoked.
     *
     * @param element The element that arrived from the first input.
     */
    void onElement1(final IN1 element) throws Exception;

    /**
     * Called for every element that gets added to the bundle from the second input. If the trigger
     * decides to start evaluate, {@link BundleTriggerCallback#finishBundle()} should be invoked.
     *
     * @param element The element that arrived from the second input.
     */
    void onElement2(final IN2 element) throws Exception;

    /** Reset the trigger to its initiate status. */
    void reset();

    String explain();
}
