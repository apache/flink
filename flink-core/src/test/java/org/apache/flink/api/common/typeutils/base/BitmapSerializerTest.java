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
import org.apache.flink.types.bitmap.Bitmap;

/** A test for the {@link BitmapSerializer}. */
class BitmapSerializerTest extends SerializerTestBase<Bitmap> {

    @Override
    protected TypeSerializer<Bitmap> createSerializer() {
        return BitmapSerializer.INSTANCE;
    }

    @Override
    protected int getLength() {
        return -1;
    }

    @Override
    protected Class<Bitmap> getTypeClass() {
        return Bitmap.class;
    }

    @Override
    protected Bitmap[] getTestData() {
        Bitmap bm1 = Bitmap.empty();

        Bitmap bm2 = Bitmap.empty();
        bm2.add(1L, 100L);

        Bitmap bm3 = Bitmap.empty();
        bm3.add(10000);
        bm3.add(0xFFFF + 1);

        Bitmap bm4 = Bitmap.empty();
        bm4.addN(new int[] {1, Integer.MAX_VALUE, Integer.MIN_VALUE}, 0, 3);

        Bitmap bm5 = Bitmap.empty();
        bm5.add(0L, Integer.MAX_VALUE);

        return new Bitmap[] {bm1, bm2, bm3, bm4, bm5};
    }
}
