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

package org.apache.flink.table.runtime.generated;

import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.functions.FunctionContext;

/**
 * A function wrapper for {@link FunctionContext} if the source supports {@link
 * org.apache.flink.table.connector.source.abilities.SupportsWatermarkPushDown}.
 *
 * <p>See more in {@link WatermarkGeneratorFunctionContext}.
 */
public class WatermarkGeneratorCodeGeneratorFunctionContextWrapper extends FunctionContext {
    private final WatermarkGeneratorSupplier.Context context;

    public WatermarkGeneratorCodeGeneratorFunctionContextWrapper(
            WatermarkGeneratorSupplier.Context context) {
        super(null, null, null);
        this.context = context;
    }

    @Override
    public MetricGroup getMetricGroup() {
        return context.getMetricGroup();
    }
}
