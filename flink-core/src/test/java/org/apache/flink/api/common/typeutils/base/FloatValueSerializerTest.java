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
import org.apache.flink.types.FloatValue;

import java.util.Random;

/** A test for the {@link FloatValueSerializer}. */
public class FloatValueSerializerTest extends SerializerTestBase<FloatValue> {

    @Override
    protected TypeSerializer<FloatValue> createSerializer() {
        return new FloatValueSerializer();
    }

    @Override
    protected int getLength() {
        return 4;
    }

    @Override
    protected Class<FloatValue> getTypeClass() {
        return FloatValue.class;
    }

    @Override
    protected FloatValue[] getTestData() {
        Random rnd = new Random(874597969123412341L);
        float rndFloat = rnd.nextFloat() * Float.MAX_VALUE;

        return new FloatValue[] {
            new FloatValue(0),
            new FloatValue(1),
            new FloatValue(-1),
            new FloatValue(Float.MAX_VALUE),
            new FloatValue(Float.MIN_VALUE),
            new FloatValue(rndFloat),
            new FloatValue(-rndFloat),
            new FloatValue(Float.NaN),
            new FloatValue(Float.NEGATIVE_INFINITY),
            new FloatValue(Float.POSITIVE_INFINITY)
        };
    }
}
