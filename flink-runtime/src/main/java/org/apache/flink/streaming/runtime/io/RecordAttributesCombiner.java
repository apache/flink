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

/** RecordAttributesValve combine RecordAttributes from different input channels. */
public class RecordAttributesCombiner {

    private static final Logger LOG = LoggerFactory.getLogger(RecordAttributesCombiner.class);

    private final RecordAttributes[] allChannelRecordAttributes;
    private int backlogChannelCnt = 0;
    private int backlogUndefinedChannelCnt;
    private RecordAttributes lastOutputAttributes = null;

    public RecordAttributesCombiner(int numInputChannels) {
        this.backlogUndefinedChannelCnt = numInputChannels;
        this.allChannelRecordAttributes = new RecordAttributes[numInputChannels];
    }

    public void inputRecordAttributes(
            RecordAttributes recordAttributes, int channelIdx, DataOutput<?> output)
            throws Exception {
        LOG.debug("RecordAttributes: {} from channel idx: {}", recordAttributes, channelIdx);
        RecordAttributes lastChannelRecordAttributes = allChannelRecordAttributes[channelIdx];
        allChannelRecordAttributes[channelIdx] = recordAttributes;

        // skip if the input RecordAttributes of the input channel is the same as the last.
        if (recordAttributes.equals(lastChannelRecordAttributes)) {
            return;
        }

        final RecordAttributesBuilder builder =
                new RecordAttributesBuilder(Collections.emptyList());

        Boolean isBacklog = combineIsBacklog(lastChannelRecordAttributes, recordAttributes);
        if (isBacklog == null) {
            if (lastOutputAttributes == null) {
                return;
            } else {
                isBacklog = lastOutputAttributes.isBacklog();
            }
        }
        builder.setBacklog(isBacklog);

        final RecordAttributes outputAttribute = builder.build();
        if (!outputAttribute.equals(lastOutputAttributes)) {
            output.emitRecordAttributes(outputAttribute);
            lastOutputAttributes = outputAttribute;
        }
    }

    /**
     * If any of the input channels is backlog, the combined RecordAttributes is backlog. Return
     * null if the isBacklog cannot be determined, i.e. when none of the input channel is processing
     * backlog and some input channels are undefined.
     */
    private Boolean combineIsBacklog(
            RecordAttributes lastRecordAttributes, RecordAttributes recordAttributes) {

        if (lastRecordAttributes == null) {
            backlogUndefinedChannelCnt--;
            if (recordAttributes.isBacklog()) {
                backlogChannelCnt++;
            }
        } else if (lastRecordAttributes.isBacklog() != recordAttributes.isBacklog()) {
            if (recordAttributes.isBacklog()) {
                backlogChannelCnt++;
            } else {
                backlogChannelCnt--;
            }
        }

        // The input is processing backlog if any channel is processing backlog
        if (backlogChannelCnt > 0) {
            return true;
        }

        // None of the input channel is processing backlog and some are undefined
        if (backlogUndefinedChannelCnt > 0) {
            return null;
        }

        // All the input channels are defined and not processing backlog
        return false;
    }
}
