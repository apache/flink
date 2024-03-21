/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.taskprocessing;

import org.apache.flink.annotation.Internal;

/**
 * Enumeration for the execution order under asynchronous state APIs. For synchronous state APIs,
 * the execution order is always {@link #ELEMENT_ORDER}. {@link #STATE_ORDER} generally has higher
 * performance than {@link #ELEMENT_ORDER}. Note: {@link #STATE_ORDER} is an advance option, please
 * make sure you are aware of possible out-of-order situations under asynchronous state APIs.
 */
@Internal
public enum OrderPreserveMode {
    /** The records with same keys are strictly processed in order of arrival. */
    ELEMENT_ORDER,
    /**
     * For same-key records, state requests and subsequent callbacks are processed in the order in
     * which each record makes its first state request. But the code before the first state request
     * for each record can be processed out-of-order with requests from other records.
     */
    STATE_ORDER
}
