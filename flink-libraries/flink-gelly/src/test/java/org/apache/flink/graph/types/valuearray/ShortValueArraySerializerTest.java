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
import org.apache.flink.types.ShortValue;

import java.util.Random;

/** A test for the {@link ShortValueArraySerializer}. */
public class ShortValueArraySerializerTest extends ValueArraySerializerTestBase<ShortValueArray> {

    @Override
    protected TypeSerializer<ShortValueArray> createSerializer() {
        return new ShortValueArraySerializer();
    }

    @Override
    protected int getLength() {
        return -1;
    }

    @Override
    protected Class<ShortValueArray> getTypeClass() {
        return ShortValueArray.class;
    }

    @Override
    protected ShortValueArray[] getTestData() {
        int defaultElements =
                ShortValueArray.DEFAULT_CAPACITY_IN_BYTES / ShortValueArray.ELEMENT_LENGTH_IN_BYTES;

        Random rnd = new Random(874597969123412341L);
        int rndLong = rnd.nextInt();

        ShortValueArray lva0 = new ShortValueArray();

        ShortValueArray lva1 = new ShortValueArray();
        lva1.addAll(lva0);
        lva1.add(new ShortValue((short) 0));

        ShortValueArray lva2 = new ShortValueArray();
        lva2.addAll(lva1);
        lva2.add(new ShortValue((short) 1));

        ShortValueArray lva3 = new ShortValueArray();
        lva3.addAll(lva2);
        lva3.add(new ShortValue((short) -1));

        ShortValueArray lva4 = new ShortValueArray();
        lva4.addAll(lva3);
        lva4.add(new ShortValue(Short.MAX_VALUE));

        ShortValueArray lva5 = new ShortValueArray();
        lva5.addAll(lva4);
        lva5.add(new ShortValue(Short.MIN_VALUE));

        ShortValueArray lva6 = new ShortValueArray();
        lva6.addAll(lva5);
        lva6.add(new ShortValue((short) rndLong));

        ShortValueArray lva7 = new ShortValueArray();
        lva7.addAll(lva6);
        lva7.add(new ShortValue((short) -rndLong));

        ShortValueArray lva8 = new ShortValueArray();
        lva8.addAll(lva7);
        for (int i = 0; i < 1.5 * defaultElements; i++) {
            lva8.add(new ShortValue((short) i));
        }
        lva8.addAll(lva8);

        return new ShortValueArray[] {lva0, lva1, lva2, lva3, lva4, lva5, lva6, lva7, lva8};
    }
}
