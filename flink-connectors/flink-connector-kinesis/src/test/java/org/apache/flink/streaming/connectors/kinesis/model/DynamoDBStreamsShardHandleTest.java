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

package org.apache.flink.streaming.connectors.kinesis.model;

import org.junit.jupiter.api.Test;

import static org.apache.flink.streaming.connectors.kinesis.model.DynamoDBStreamsShardHandle.SHARDID_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Shard handle unit tests. */
public class DynamoDBStreamsShardHandleTest {
    @Test
    public void testIsValidShardId() {
        // normal form
        String shardId = "shardId-00000001536805703746-69688cb1";
        assertThat(DynamoDBStreamsShardHandle.isValidShardId(shardId)).isTrue();

        // short form
        shardId = "shardId-00000001536805703746";
        assertThat(DynamoDBStreamsShardHandle.isValidShardId(shardId)).isTrue();

        // long form
        shardId = "shardId-00000001536805703746-69688cb1aljkwerijfl8228sl12a123akfla";
        assertThat(DynamoDBStreamsShardHandle.isValidShardId(shardId)).isTrue();

        // invalid with wrong prefix
        shardId = "sId-00000001536805703746-69688cb1";
        assertThat(DynamoDBStreamsShardHandle.isValidShardId(shardId)).isFalse();

        // invalid with non-digits
        shardId = "shardId-0000000153680570aabb-69688cb1";
        assertThat(DynamoDBStreamsShardHandle.isValidShardId(shardId)).isFalse();

        // invalid with shardId too long
        shardId = "shardId-00000001536805703746-69688cb1aljkwerijfl8228sl12a123akfla0000";
        assertThat(DynamoDBStreamsShardHandle.isValidShardId(shardId)).isFalse();
    }

    @Test
    public void testCompareShardId() {
        final int numShardIds = 10;
        final int shardIdDigitLen = 20;
        final String zeros = "00000000000000000000"; // twenty '0' chars
        String shardIdValid = "shardId-00000001536805703746-69688cb1";
        String shardIdInvalid = "shardId-0000000153680570aabb-69688cb1";

        assertThat(DynamoDBStreamsShardHandle.compareShardIds(shardIdValid, shardIdValid))
                .isEqualTo(0);

        // comparison of invalid shardIds should yield exception
        assertThatThrownBy(
                        () ->
                                DynamoDBStreamsShardHandle.compareShardIds(
                                        shardIdValid, shardIdInvalid))
                .as("invalid shard Id" + shardIdInvalid + " should trigger exception")
                .isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(
                        () ->
                                DynamoDBStreamsShardHandle.compareShardIds(
                                        shardIdInvalid, shardIdValid))
                .as("invalid shard Id" + shardIdInvalid + " should trigger exception")
                .isInstanceOf(IllegalArgumentException.class);

        // compare randomly generated shardIds based on timestamp
        String[] shardIds = new String[numShardIds];
        for (int i = 0; i < numShardIds; i++) {
            String nowStr = String.valueOf(System.currentTimeMillis());
            if (nowStr.length() < shardIdDigitLen) {
                shardIds[i] =
                        SHARDID_PREFIX
                                + zeros.substring(0, shardIdDigitLen - nowStr.length())
                                + nowStr;
            } else {
                shardIds[i] = SHARDID_PREFIX + nowStr.substring(0, shardIdDigitLen);
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                // ignore
            }
        }
        for (int i = 1; i < numShardIds - 1; i++) {
            assertThat(DynamoDBStreamsShardHandle.compareShardIds(shardIds[i - 1], shardIds[i]))
                    .isLessThan(0);
            assertThat(DynamoDBStreamsShardHandle.compareShardIds(shardIds[i], shardIds[i]))
                    .isEqualTo(0);
            assertThat(DynamoDBStreamsShardHandle.compareShardIds(shardIds[i], shardIds[i + 1]))
                    .isLessThan(0);
        }
    }
}
