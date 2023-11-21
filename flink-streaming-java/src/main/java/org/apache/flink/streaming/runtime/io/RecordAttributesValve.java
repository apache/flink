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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.streaming.runtime.io.PushingAsyncDataInput.DataOutput;
import org.apache.flink.streaming.runtime.streamrecord.RecordAttributes;
import org.apache.flink.streaming.runtime.streamrecord.RecordAttributesBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

/**
 * RecordAttributesValve combine RecordAttributes from different input channels. If any of the input
 * channels is non backlog, the combined RecordAttributes is non backlog.
 *
 * <p>Currently, we only support switching the backlog status from null to backlog and backlog to
 * non-backlog. Switching from non-backlog to backlog is not support at the moment, and it will be
 * ignored.
 */
public class RecordAttributesValve {

    private static final Logger LOG = LoggerFactory.getLogger(RecordAttributesValve.class);

    private final int numInputChannels;
    private final RecordAttributes[] allChannelRecordAttributes;

    private int backlogChannelsCnt = 0;
    private RecordAttributes lastOutputAttributes = null;

    public RecordAttributesValve(int numInputChannels) {
        this.numInputChannels = numInputChannels;
        this.allChannelRecordAttributes = new RecordAttributes[numInputChannels];
    }

    public void inputRecordAttributes(
            RecordAttributes recordAttributes, int channelIdx, DataOutput<?> output)
            throws Exception {
        LOG.debug("RecordAttributes: {} from channel idx: {}", recordAttributes, channelIdx);
        RecordAttributes lastChannelRecordAttributes = allChannelRecordAttributes[channelIdx];
        allChannelRecordAttributes[channelIdx] = recordAttributes;

        if (lastChannelRecordAttributes == null) {
            lastChannelRecordAttributes =
                    new RecordAttributesBuilder(Collections.emptyList()).build();
        }

        if (lastChannelRecordAttributes.isBacklog() == recordAttributes.isBacklog()) {
            return;
        }

        if (recordAttributes.isBacklog()) {
            backlogChannelsCnt += 1;
        } else {
            backlogChannelsCnt -= 1;
        }

        if (lastOutputAttributes == null && backlogChannelsCnt != numInputChannels) {
            return;
        }

        final RecordAttributesBuilder builder =
                new RecordAttributesBuilder(Collections.emptyList());
        builder.setBacklog(backlogChannelsCnt >= numInputChannels);
        final RecordAttributes outputAttribute = builder.build();
        if (lastOutputAttributes == null
                || lastOutputAttributes.isBacklog() != outputAttribute.isBacklog()) {
            if (lastOutputAttributes != null && !lastOutputAttributes.isBacklog()) {
                LOG.warn(
                        "Switching from non-backlog to backlog is currently not supported. Backlog status remains.");
                return;
            }
            lastOutputAttributes = outputAttribute;
            output.emitRecordAttributes(outputAttribute);
        }
    }
}
