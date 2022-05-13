/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.graph.types.valuearray;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.types.FloatValue;

import java.util.Random;

/** A test for the {@link FloatValueArraySerializer}. */
public class FloatValueArraySerializerTest extends ValueArraySerializerTestBase<FloatValueArray> {

    @Override
    protected TypeSerializer<FloatValueArray> createSerializer() {
        return new FloatValueArraySerializer();
    }

    @Override
    protected int getLength() {
        return -1;
    }

    @Override
    protected Class<FloatValueArray> getTypeClass() {
        return FloatValueArray.class;
    }

    @Override
    protected FloatValueArray[] getTestData() {
        int defaultElements =
                FloatValueArray.DEFAULT_CAPACITY_IN_BYTES / FloatValueArray.ELEMENT_LENGTH_IN_BYTES;

        Random rnd = new Random(874597969123412341L);
        int rndLong = rnd.nextInt();

        FloatValueArray lva0 = new FloatValueArray();

        FloatValueArray lva1 = new FloatValueArray();
        lva1.addAll(lva0);
        lva1.add(new FloatValue(0));

        FloatValueArray lva2 = new FloatValueArray();
        lva2.addAll(lva1);
        lva2.add(new FloatValue(1));

        FloatValueArray lva3 = new FloatValueArray();
        lva3.addAll(lva2);
        lva3.add(new FloatValue(-1));

        FloatValueArray lva4 = new FloatValueArray();
        lva4.addAll(lva3);
        lva4.add(new FloatValue(Float.MAX_VALUE));

        FloatValueArray lva5 = new FloatValueArray();
        lva5.addAll(lva4);
        lva5.add(new FloatValue(Float.MIN_VALUE));

        FloatValueArray lva6 = new FloatValueArray();
        lva6.addAll(lva5);
        lva6.add(new FloatValue(rndLong));

        FloatValueArray lva7 = new FloatValueArray();
        lva7.addAll(lva6);
        lva7.add(new FloatValue(-rndLong));

        FloatValueArray lva8 = new FloatValueArray();
        lva8.addAll(lva7);
        for (int i = 0; i < 1.5 * defaultElements; i++) {
            lva8.add(new FloatValue(i));
        }
        lva8.addAll(lva8);

        return new FloatValueArray[] {lva0, lva1, lva2, lva3, lva4, lva5, lva6, lva7, lva8};
    }
}
