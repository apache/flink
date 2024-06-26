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

package org.apache.flink.api.common.eventtime;

import org.apache.flink.api.common.WatermarkDeclaration;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

public class TimestampWatermarkDeclaration implements WatermarkDeclaration.WatermarkSerde {
    @Override
    public Class<TimestampWatermark> watermarkClass() {
        return TimestampWatermark.class;
    }

    @Override
    public void serialize(Watermark genericWatermark, DataOutputView target) throws IOException {
        target.writeLong(((TimestampWatermark) genericWatermark).getTimestamp());
    }

    @Override
    public Watermark deserialize(DataInputView inputView) throws IOException {
        long ts = inputView.readLong();
        return new TimestampWatermark(ts);
    }
}
