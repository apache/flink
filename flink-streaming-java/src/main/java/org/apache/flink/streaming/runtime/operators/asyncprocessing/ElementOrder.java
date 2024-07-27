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

package org.apache.flink.streaming.runtime.operators.asyncprocessing;

import org.apache.flink.annotation.Internal;

/**
 * This enum defines the element order of being processed. Only the elements with the same key
 * should be considered here. We should keep this internal and away from API module for now, until
 * we could see the concrete need for {@link #FIRST_STATE_ORDER} from average users.
 */
@Internal
public enum ElementOrder {
    /**
     * Treat the record processing as a whole, meaning that any {@code processElement} call for the
     * elements with same key should follow the order of record arrival AND no parallel run is
     * allowed.
     */
    RECORD_ORDER,

    /**
     * The {@code processElement} call will be invoked on record arrival, but may be blocked at the
     * first state accessing if there is a preceding same-key record under processing.
     */
    FIRST_STATE_ORDER,
}
