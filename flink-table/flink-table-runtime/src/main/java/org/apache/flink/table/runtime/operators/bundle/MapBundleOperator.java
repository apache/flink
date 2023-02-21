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

package org.apache.flink.table.runtime.operators.bundle;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.table.runtime.operators.bundle.trigger.BundleTrigger;

/**
 * The {@link MapBundleOperator} uses a {@link KeySelector} to extract bundle key, thus can be used
 * with non-keyed-stream.
 */
public class MapBundleOperator<K, V, IN, OUT> extends AbstractMapBundleOperator<K, V, IN, OUT> {

    private static final long serialVersionUID = 1L;

    /** KeySelector is used to extract key for bundle map. */
    private final KeySelector<IN, K> keySelector;

    public MapBundleOperator(
            MapBundleFunction<K, V, IN, OUT> function,
            BundleTrigger<IN> bundleTrigger,
            KeySelector<IN, K> keySelector) {
        super(function, bundleTrigger);
        this.keySelector = keySelector;
    }

    @Override
    protected K getKey(IN input) throws Exception {
        return this.keySelector.getKey(input);
    }
}
