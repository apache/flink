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
import org.apache.flink.types.ByteValue;

import java.util.Random;

/** A test for the {@link ByteValueArraySerializer}. */
public class ByteValueArraySerializerTest extends ValueArraySerializerTestBase<ByteValueArray> {

    @Override
    protected TypeSerializer<ByteValueArray> createSerializer() {
        return new ByteValueArraySerializer();
    }

    @Override
    protected int getLength() {
        return -1;
    }

    @Override
    protected Class<ByteValueArray> getTypeClass() {
        return ByteValueArray.class;
    }

    @Override
    protected ByteValueArray[] getTestData() {
        int defaultElements =
                ByteValueArray.DEFAULT_CAPACITY_IN_BYTES / ByteValueArray.ELEMENT_LENGTH_IN_BYTES;

        Random rnd = new Random(874597969123412341L);
        int rndLong = rnd.nextInt();

        ByteValueArray lva0 = new ByteValueArray();

        ByteValueArray lva1 = new ByteValueArray();
        lva1.addAll(lva0);
        lva1.add(new ByteValue((byte) 0));

        ByteValueArray lva2 = new ByteValueArray();
        lva2.addAll(lva1);
        lva2.add(new ByteValue((byte) 1));

        ByteValueArray lva3 = new ByteValueArray();
        lva3.addAll(lva2);
        lva3.add(new ByteValue((byte) -1));

        ByteValueArray lva4 = new ByteValueArray();
        lva4.addAll(lva3);
        lva4.add(new ByteValue(Byte.MAX_VALUE));

        ByteValueArray lva5 = new ByteValueArray();
        lva5.addAll(lva4);
        lva5.add(new ByteValue(Byte.MIN_VALUE));

        ByteValueArray lva6 = new ByteValueArray();
        lva6.addAll(lva5);
        lva6.add(new ByteValue((byte) rndLong));

        ByteValueArray lva7 = new ByteValueArray();
        lva7.addAll(lva6);
        lva7.add(new ByteValue((byte) -rndLong));

        ByteValueArray lva8 = new ByteValueArray();
        lva8.addAll(lva7);
        for (int i = 0; i < 1.5 * defaultElements; i++) {
            lva8.add(new ByteValue((byte) i));
        }
        lva8.addAll(lva8);

        return new ByteValueArray[] {lva0, lva1, lva2, lva3, lva4, lva5, lva6, lva7, lva8};
    }
}
