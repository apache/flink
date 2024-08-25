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

import org.apache.flink.annotation.Internal;

@Internal
public class ReusableWatermarkContext
        implements InternalWatermarkDeclaration.WatermarkCombiner.Context {

    private int numChannels;
    private int currentChannel;

    public ReusableWatermarkContext(int numChannels, int currentChannel) {
        this.numChannels = numChannels;
        this.currentChannel = currentChannel;
    }

    public void setChannelInfo(int numChannels, int currentChannel) {
        this.numChannels = numChannels;
        this.currentChannel = currentChannel;
    }

    @Override
    public int getNumberOfInputChannels() {
        return numChannels;
    }

    @Override
    public int getIndexOfCurrentChannel() {
        return currentChannel;
    }
}
