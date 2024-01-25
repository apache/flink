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

package org.apache.flink.table.runtime.operators.window.tvf.common;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.runtime.operators.window.groupwindow.assigners.GroupWindowAssigner;

import java.io.Serializable;

/**
 * WindowAssigner is used to assign windows to elements.
 *
 * <p>The differences between {@link WindowAssigner} and {@link GroupWindowAssigner} is that, this
 * window assigner is translated from the new window TVF syntax, but the other is from the legacy
 * GROUP WINDOW FUNCTION syntax. In the long future, {@link GroupWindowAssigner} will be dropped.
 *
 * <p>See more details in {@link WindowAggOperator}.
 */
@Internal
public interface WindowAssigner extends Serializable {

    /**
     * Returns {@code true} if elements are assigned to windows based on event time, {@code false}
     * based on processing time.
     */
    boolean isEventTime();

    /** Returns a description of this window assigner. */
    String getDescription();
}
