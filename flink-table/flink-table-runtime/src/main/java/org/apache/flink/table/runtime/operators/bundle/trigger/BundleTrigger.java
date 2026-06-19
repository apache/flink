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
 * A {@link BundleTrigger} determines when a bundle of input elements should be evaluated and
 * trigger the callback which registered previously.
 *
 * @param <T> The input element type.
 */
@Internal
public interface BundleTrigger<T> extends Serializable {

    /** Register a callback which will be called once this trigger decides to finish this bundle. */
    void registerCallback(BundleTriggerCallback callback);

    /**
     * Called for every element that gets added to the bundle. If the trigger decides to start
     * evaluate the input, {@link BundleTriggerCallback#finishBundle()} should be invoked.
     *
     * @param element The element that arrived.
     */
    void onElement(final T element) throws Exception;

    /** Reset the trigger to its initiate status. */
    void reset();

    String explain();
}
