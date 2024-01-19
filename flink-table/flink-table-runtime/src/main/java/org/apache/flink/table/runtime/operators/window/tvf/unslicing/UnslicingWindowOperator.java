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

package org.apache.flink.table.runtime.operators.window.tvf.unslicing;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.runtime.operators.window.tvf.common.WindowOperatorBase;

/**
 * The {@link UnslicingWindowOperator} implements an optimized processing for unaligned windows.
 *
 * <h3>Abstraction of Unslicing Window Operator</h3>
 *
 * <p>An unslicing window operator is a simple wrap of {@link UnslicingWindowProcessor}. It
 * delegates all the important methods to the underlying processor.
 *
 * <p>A {@link UnslicingWindowProcessor} usually leverages the {@link UnsliceAssigner} to assign
 * slices and calculate based on the window.
 *
 * <p>Note: Currently, the {@link UnslicingWindowOperator} only support session time window.
 */
@Internal
public class UnslicingWindowOperator<K, W> extends WindowOperatorBase<K, W> {

    private static final long serialVersionUID = 1L;

    public UnslicingWindowOperator(UnslicingWindowProcessor<W> windowProcessor) {
        super(windowProcessor);
    }
}
