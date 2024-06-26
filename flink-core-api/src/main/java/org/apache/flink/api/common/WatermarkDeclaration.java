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

/**
 * This class defines watermark handling policy for ProcessOperator. Note that implementations of
 * this interface must ensure to provide the default constructor.
 */
public interface WatermarkDeclaration extends Serializable {
    /** Declare generated watermarks by its operator upfront. */
    WatermarkSerde declaredWatermark();

    /** Returns user-defined Watermark combiner implementation. */
    WatermarkCombiner watermarkCombiner();

    /**
     * Declaration for Watermark classes. Note that the subclasses of this interface should ensure
     * zero-argument constructor.
     */
    interface WatermarkSerde extends Serializable {
        Class<? extends Watermark> watermarkClass();

        void serialize(Watermark genericWatermark, DataOutputView target) throws IOException;

        Watermark deserialize(DataInputView inputView) throws IOException;
    }
}
