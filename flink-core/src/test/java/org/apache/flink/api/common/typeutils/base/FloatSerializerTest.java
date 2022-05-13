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

import java.util.Random;

/** A test for the {@link org.apache.flink.api.common.typeutils.base.FloatSerializer}. */
public class FloatSerializerTest extends SerializerTestBase<Float> {

    @Override
    protected TypeSerializer<Float> createSerializer() {
        return new FloatSerializer();
    }

    @Override
    protected int getLength() {
        return 4;
    }

    @Override
    protected Class<Float> getTypeClass() {
        return Float.class;
    }

    @Override
    protected Float[] getTestData() {
        Random rnd = new Random(874597969123412341L);
        float rndFloat = rnd.nextFloat() * Float.MAX_VALUE;

        return new Float[] {
            Float.valueOf(0),
            Float.valueOf(1),
            Float.valueOf(-1),
            Float.valueOf(Float.MAX_VALUE),
            Float.valueOf(Float.MIN_VALUE),
            Float.valueOf(rndFloat),
            Float.valueOf(-rndFloat),
            Float.valueOf(Float.NaN),
            Float.valueOf(Float.NEGATIVE_INFINITY),
            Float.valueOf(Float.POSITIVE_INFINITY)
        };
    }
}
