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

package org.apache.flink.api.common;

import org.apache.flink.annotation.Internal;

/** An enum to represent the tasks scheduling strategy. */
@Internal
public enum TaskSchedulingStrategy {

    /**
     * Local-input-preferred (default) tasks scheduling strategy represents that the scheduler will
     * try its best to reduce the remote data exchange when scheduling tasks.
     */
    LOCAL_INPUT_PREFERRED,

    /**
     * Balanced-preferred tasks scheduling strategy represents that the scheduler will try its best
     * to ensure balanced tasks scheduling when scheduling tasks.
     */
    BALANCED_PREFERRED
}
