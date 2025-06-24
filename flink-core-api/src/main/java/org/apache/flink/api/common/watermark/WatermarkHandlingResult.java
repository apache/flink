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

package org.apache.flink.api.common.watermark;

import org.apache.flink.annotation.Experimental;

/** This class defines watermark handling result for user-defined process function. */
@Experimental
public enum WatermarkHandlingResult {
    /**
     * Process function only peek the watermark, and it's framework's responsibility to handle this
     * watermark.
     */
    PEEK,

    /**
     * This watermark should be sent to downstream by process function itself. The framework does no
     * additional processing.
     */
    POLL,
}
