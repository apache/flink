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

package org.apache.flink.runtime.io.network;

import org.apache.flink.runtime.io.network.api.writer.ChannelSelector;
import org.apache.flink.runtime.io.network.api.writer.RoundRobinChannelSelector;
import org.apache.flink.types.StringValue;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/** This class checks the functionality of the {@link RoundRobinChannelSelector} class. */
public class DefaultChannelSelectorTest {

    /** This test checks the channel selection. */
    @Test
    public void channelSelect() {
        final StringValue dummyRecord = new StringValue("abc");
        final RoundRobinChannelSelector<StringValue> selector = new RoundRobinChannelSelector<>();
        selector.setup(2);

        assertSelectedChannel(selector, dummyRecord, 0);
        assertSelectedChannel(selector, dummyRecord, 1);
    }

    private void assertSelectedChannel(
            ChannelSelector<StringValue> selector, StringValue record, int expectedChannel) {

        int actualResult = selector.selectChannel(record);
        assertEquals(expectedChannel, actualResult);
    }
}
