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

package org.apache.flink.datastream.impl.watermark;

import org.apache.flink.api.common.watermark.Watermark;
import org.apache.flink.api.common.watermark.WatermarkManager;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.GeneralizedWatermarkElement;
import org.apache.flink.util.Preconditions;

import java.util.Set;

/** A default implementation of {@link WatermarkManager}. */
public class DefaultWatermarkManager implements WatermarkManager {

    private final Output<?> streamRecordOutput;

    private final Set<String> declaredWatermarkIdentifiers;

    public DefaultWatermarkManager(
            Output<?> streamRecordOutput, Set<String> declaredWatermarkIdentifiers) {
        this.streamRecordOutput = streamRecordOutput;
        this.declaredWatermarkIdentifiers = declaredWatermarkIdentifiers;
    }

    @Override
    public void emitWatermark(Watermark watermark) {
        Preconditions.checkState(
                declaredWatermarkIdentifiers.contains(watermark.getIdentifier()),
                "Watermark identifier "
                        + watermark.getIdentifier()
                        + " does not exist, please declare it.");
        streamRecordOutput.emitGeneralizedWatermark(new GeneralizedWatermarkElement(watermark));
    }
}
