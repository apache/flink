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
import org.apache.flink.types.CharValue;

import java.util.Random;

/** A test for the {@link CharValueSerializer}. */
public class CharValueSerializerTest extends SerializerTestBase<CharValue> {

    @Override
    protected TypeSerializer<CharValue> createSerializer() {
        return new CharValueSerializer();
    }

    @Override
    protected int getLength() {
        return 2;
    }

    @Override
    protected Class<CharValue> getTypeClass() {
        return CharValue.class;
    }

    @Override
    protected CharValue[] getTestData() {
        Random rnd = new Random(874597969123412341L);
        int rndInt = rnd.nextInt((int) Character.MAX_VALUE);

        return new CharValue[] {
            new CharValue('a'),
            new CharValue('@'),
            new CharValue('ä'),
            new CharValue('1'),
            new CharValue((char) rndInt),
            new CharValue(Character.MAX_VALUE),
            new CharValue(Character.MIN_VALUE)
        };
    }
}
