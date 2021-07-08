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

package org.apache.flink.testutils.serialization.types;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Random;

/** Utility class to help serialization for testing. */
public final class Util {

    private static final long SEED = 64871654635745873L;

    private static Random random = new Random(SEED);

    public static SerializationTestType randomRecord(SerializationTestTypeFactory type) {
        return type.factory().getRandom(Util.random);
    }

    public static MockRecords randomRecords(
            final int numElements, final SerializationTestTypeFactory type) {

        return new MockRecords(numElements) {
            @Override
            protected SerializationTestType getRecord() {
                return type.factory().getRandom(Util.random);
            }
        };
    }

    public static MockRecords randomRecords(final int numElements) {

        return new MockRecords(numElements) {
            @Override
            protected SerializationTestType getRecord() {
                // select random test type factory
                SerializationTestTypeFactory[] types = SerializationTestTypeFactory.values();
                int i = Util.random.nextInt(types.length);

                return types[i].factory().getRandom(Util.random);
            }
        };
    }

    // -----------------------------------------------------------------------------------------------------------------
    public abstract static class MockRecords implements Iterable<SerializationTestType> {

        private int numRecords;

        public MockRecords(int numRecords) {
            this.numRecords = numRecords;
        }

        @Override
        public Iterator<SerializationTestType> iterator() {
            return new Iterator<SerializationTestType>() {
                @Override
                public boolean hasNext() {
                    return numRecords > 0;
                }

                @Override
                public SerializationTestType next() {
                    if (numRecords > 0) {
                        numRecords--;

                        return getRecord();
                    }

                    throw new NoSuchElementException();
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
        }

        protected abstract SerializationTestType getRecord();
    }

    /** No instantiation. */
    private Util() {
        throw new RuntimeException();
    }
}
