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

package org.apache.flink.runtime.watermark;

import org.apache.flink.api.watermark.BoolWatermark;
import org.apache.flink.api.watermark.GeneralizedWatermark;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

public class BoolWatermarkSerde implements InternalWatermarkDeclaration.WatermarkSerde {
    @Override
    public Class<? extends GeneralizedWatermark> watermarkClass() {
        return BoolWatermark.class;
    }

    @Override
    public void serialize(GeneralizedWatermark genericWatermark, DataOutputView target)
            throws IOException {
        target.writeBoolean(((BoolWatermark) genericWatermark).getValue());
        target.writeUTF(genericWatermark.getIdentifier());
    }

    @Override
    public GeneralizedWatermark deserialize(DataInputView inputView) throws IOException {
        return new BoolWatermark(inputView.readBoolean(), inputView.readUTF());
    }
}
