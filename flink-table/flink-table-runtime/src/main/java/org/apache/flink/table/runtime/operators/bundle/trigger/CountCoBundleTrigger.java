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

import org.apache.flink.util.Preconditions;

/**
 * A {@link CoBundleTrigger} that fires once the count of elements in a bundle reaches the given
 * count.
 */
public class CountCoBundleTrigger<IN1, IN2> implements CoBundleTrigger<IN1, IN2> {

    private final long maxCount;
    private transient BundleTriggerCallback callback;
    private transient long count = 0;

    public CountCoBundleTrigger(long maxCount) {
        Preconditions.checkArgument(maxCount > 0, "maxCount must be greater than 0");
        this.maxCount = maxCount;
    }

    @Override
    public void registerCallback(BundleTriggerCallback callback) {
        this.callback = Preconditions.checkNotNull(callback, "callback is null");
    }

    @Override
    public void onElement1(final IN1 element) throws Exception {
        count++;
        if (count >= maxCount) {
            callback.finishBundle();
            reset();
        }
    }

    @Override
    public void onElement2(final IN2 element) throws Exception {
        count++;
        if (count >= maxCount) {
            callback.finishBundle();
            reset();
        }
    }

    @Override
    public void reset() {
        count = 0;
    }

    @Override
    public String explain() {
        return "CountCoBundleTrigger with size " + maxCount;
    }
}
