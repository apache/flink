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

import org.apache.flink.process.api.common.Collector;
import org.apache.flink.process.api.context.NonPartitionedContext;
import org.apache.flink.process.api.function.ApplyPartitionFunction;

import java.util.Iterator;

/**
 * {@link NonPartitionedContext} for keyed operator. This will take care of the key context when
 * apply to all keyed partitions.
 */
public class DefaultKeyedNonPartitionedContext<OUT> extends DefaultNonPartitionedContext<OUT> {
    private final AllKeysContext allKeysContext;

    public DefaultKeyedNonPartitionedContext(
            AllKeysContext allKeysContext,
            DefaultRuntimeContext context,
            Collector<OUT> collector) {
        super(context, collector);
        this.allKeysContext = allKeysContext;
    }

    @Override
    public void applyToAllPartitions(ApplyPartitionFunction<OUT> applyPartitionFunction)
            throws Exception {
        // for keyed operator, each key corresponds to a partition.
        for (Iterator<Object> it = allKeysContext.getAllKeysIter(); it.hasNext(); ) {
            Object key = it.next();
            context.getStateManager().setCurrentKey(key);
            applyPartitionFunction.apply(collector, context);
            context.getStateManager().resetCurrentKey();
        }
    }
}
