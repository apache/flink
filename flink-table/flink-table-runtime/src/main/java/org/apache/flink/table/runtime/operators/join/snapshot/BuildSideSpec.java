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
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.Serializable;

/**
 * Spec of how the committed build side of a {@link LateralSnapshotJoinOperator} is laid out. The
 * planner chooses the layout, {@link BuildStateViews#create} instantiates the matching {@link
 * BuildStateView}.
 */
@Internal
public class BuildSideSpec implements Serializable {

    private static final long serialVersionUID = 1L;

    /** The layout of the committed build side. */
    public enum Layout {
        /** Row to count multi-set, for a build side without an upsert key. */
        MULTI_SET,
        /** One row per upsert key, for a build side with an upsert key containing the join key. */
        UPSERT
    }

    private final Layout layout;

    /** Field indices of the build-side upsert key, only set for {@link Layout#UPSERT}. */
    @Nullable private final int[] upsertKeys;

    private BuildSideSpec(Layout layout, @Nullable int[] upsertKeys) {
        this.layout = layout;
        this.upsertKeys = upsertKeys;
    }

    public static BuildSideSpec multiSet() {
        return new BuildSideSpec(Layout.MULTI_SET, null);
    }

    public static BuildSideSpec upsert(int[] upsertKeys) {
        return new BuildSideSpec(Layout.UPSERT, Preconditions.checkNotNull(upsertKeys));
    }

    public Layout getLayout() {
        return layout;
    }

    public int[] getUpsertKeys() {
        return Preconditions.checkNotNull(
                upsertKeys, "Build side layout %s has no upsert keys", layout);
    }
}
