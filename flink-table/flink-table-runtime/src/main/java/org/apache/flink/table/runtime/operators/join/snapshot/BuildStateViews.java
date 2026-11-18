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

package org.apache.flink.table.runtime.operators.join.snapshot;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;

/** Factory for {@link BuildStateView}s. */
@Internal
public final class BuildStateViews {

    /** Creates the view for the layout of the given spec and declares its keyed state. */
    public static BuildStateView create(
            RuntimeContext ctx, BuildSideSpec spec, InternalTypeInfo<RowData> buildType) {
        switch (spec.getLayout()) {
            case MULTI_SET:
                return new MultiSetBuildStateView(ctx, buildType);
            case JOIN_KEY:
                return new JoinKeyBuildStateView(ctx, buildType);
            case UPSERT:
                return new UpsertBuildStateView(ctx, buildType, spec.getUpsertKeys());
            default:
                throw new IllegalStateException("Unknown build side layout: " + spec.getLayout());
        }
    }

    private BuildStateViews() {}
}
