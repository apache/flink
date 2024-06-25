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

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.Set;

/** This class defines watermark handling policy for ProcessOperator.*/
public interface WatermarkPolicy extends Serializable {

    /**
     * Define watermark responsibility. For a given watermark, this method returns WatermarkResult.
     * */
    WatermarkResult useWatermark(Watermark watermark);

    enum WatermarkResult {
        /**  Peek the watermark. The respobsibility to propagate is on the framework */
        PEEK,
        /** Pop the watermark. The responsibility to propagate (this or another watermark) is on the user function */
        POP
    }
}


