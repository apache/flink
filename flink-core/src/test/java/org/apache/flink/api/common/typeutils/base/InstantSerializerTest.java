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

package org.apache.flink.api.common.typeutils.base;

import org.apache.flink.api.common.typeutils.SerializerTestBase;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import java.time.Instant;
import java.util.Random;

/** A test for the {@link InstantSerializer}. */
public class InstantSerializerTest extends SerializerTestBase<Instant> {
    @Override
    protected TypeSerializer<Instant> createSerializer() {
        return new InstantSerializer();
    }

    @Override
    protected int getLength() {
        return 12;
    }

    @Override
    protected Class<Instant> getTypeClass() {
        return Instant.class;
    }

    private static long rndSeconds(Random rnd) {
        return (long)
                (Instant.MIN.getEpochSecond()
                        + rnd.nextDouble()
                                * (Instant.MAX.getEpochSecond() - Instant.MIN.getEpochSecond()));
    }

    private static int rndNanos(Random rnd) {
        return (int) (rnd.nextDouble() * 999999999);
    }

    @Override
    protected Instant[] getTestData() {
        final Random rnd = new Random(874597969123412341L);

        return new Instant[] {
            Instant.EPOCH,
            Instant.MIN,
            Instant.MAX,
            Instant.ofEpochSecond(rndSeconds(rnd), rndNanos(rnd)),
            Instant.ofEpochSecond(1534135584, 949495),
            Instant.ofEpochSecond(56090783)
        };
    }
}
