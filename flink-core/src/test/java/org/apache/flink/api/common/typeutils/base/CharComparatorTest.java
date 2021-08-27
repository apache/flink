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

import org.apache.flink.api.common.typeutils.ComparatorTestBase;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import java.util.Random;

public class CharComparatorTest extends ComparatorTestBase<Character> {

    @Override
    protected TypeComparator<Character> createComparator(boolean ascending) {
        return new CharComparator(ascending);
    }

    @Override
    protected TypeSerializer<Character> createSerializer() {
        return new CharSerializer();
    }

    @Override
    protected Character[] getSortedTestData() {
        Random rnd = new Random(874597969123412338L);
        int rndChar = rnd.nextInt(Character.MAX_VALUE);
        if (rndChar < 0) {
            rndChar = -rndChar;
        }
        if (rndChar == (int) Character.MIN_VALUE) {
            rndChar += 2;
        }
        if (rndChar == (int) Character.MAX_VALUE) {
            rndChar -= 2;
        }
        return new Character[] {
            Character.MIN_VALUE, (char) rndChar, (char) (rndChar + 1), Character.MAX_VALUE
        };
    }
}
