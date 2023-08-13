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

package org.apache.flink.runtime.util;

import org.apache.flink.runtime.testutils.recordutils.RecordComparator;
import org.apache.flink.runtime.testutils.recordutils.RecordSerializer;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.Record;
import org.apache.flink.types.StringValue;
import org.apache.flink.util.MutableObjectIterator;
import org.apache.flink.util.TraversableOnceException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

/**
 * Test for the key grouped iterator, which advances in windows containing the same key and provides
 * a sub-iterator over the records with the same key.
 */
class ReusingKeyGroupedIteratorTest {

    private MutableObjectIterator<Record> sourceIter; // the iterator that provides the input

    private ReusingKeyGroupedIterator<Record>
            psi; // the grouping iterator, progressing in key steps

    @BeforeEach
    void setup() {
        final ArrayList<IntStringPair> source = new ArrayList<IntStringPair>();

        // add elements to the source
        source.add(new IntStringPair(new IntValue(1), new StringValue("A")));
        source.add(new IntStringPair(new IntValue(2), new StringValue("B")));
        source.add(new IntStringPair(new IntValue(3), new StringValue("C")));
        source.add(new IntStringPair(new IntValue(3), new StringValue("D")));
        source.add(new IntStringPair(new IntValue(4), new StringValue("E")));
        source.add(new IntStringPair(new IntValue(4), new StringValue("F")));
        source.add(new IntStringPair(new IntValue(4), new StringValue("G")));
        source.add(new IntStringPair(new IntValue(5), new StringValue("H")));
        source.add(new IntStringPair(new IntValue(5), new StringValue("I")));
        source.add(new IntStringPair(new IntValue(5), new StringValue("J")));
        source.add(new IntStringPair(new IntValue(5), new StringValue("K")));
        source.add(new IntStringPair(new IntValue(5), new StringValue("L")));

        this.sourceIter =
                new MutableObjectIterator<Record>() {
                    final Iterator<IntStringPair> it = source.iterator();

                    @Override
                    public Record next(Record reuse) throws IOException {
                        if (it.hasNext()) {
                            IntStringPair pair = it.next();
                            reuse.setField(0, pair.getInteger());
                            reuse.setField(1, pair.getString());
                            return reuse;
                        } else {
                            return null;
                        }
                    }

                    @Override
                    public Record next() throws IOException {
                        if (it.hasNext()) {
                            IntStringPair pair = it.next();
                            Record result = new Record(2);
                            result.setField(0, pair.getInteger());
                            result.setField(1, pair.getString());
                            return result;
                        } else {
                            return null;
                        }
                    }
                };

        final RecordSerializer serializer = RecordSerializer.get();
        @SuppressWarnings("unchecked")
        final RecordComparator comparator =
                new RecordComparator(new int[] {0}, new Class[] {IntValue.class});

        this.psi = new ReusingKeyGroupedIterator<Record>(this.sourceIter, serializer, comparator);
    }

    @Test
    void testNextKeyOnly() throws Exception {
        try {
            assertThat(this.psi.nextKey())
                    .withFailMessage("KeyGroupedIterator must have another key.")
                    .isTrue();
            assertThat(
                            this.psi
                                    .getComparatorWithCurrentReference()
                                    .equalToReference(new Record(new IntValue(1))))
                    .withFailMessage("KeyGroupedIterator returned a wrong key.")
                    .isTrue();
            assertThat(this.psi.getCurrent().getField(0, IntValue.class).getValue())
                    .withFailMessage("KeyGroupedIterator returned a wrong key.")
                    .isOne();

            assertThat(this.psi.nextKey())
                    .withFailMessage("KeyGroupedIterator must have another key.")
                    .isTrue();
            assertThat(
                            this.psi
                                    .getComparatorWithCurrentReference()
                                    .equalToReference(new Record(new IntValue(2))))
                    .withFailMessage("KeyGroupedIterator returned a wrong key.")
                    .isTrue();
            assertThat(this.psi.getCurrent().getField(0, IntValue.class).getValue())
                    .withFailMessage("KeyGroupedIterator returned a wrong key.")
                    .isEqualTo(2);

            assertThat(this.psi.nextKey())
                    .withFailMessage("KeyGroupedIterator must have another key.")
                    .isTrue();
            assertThat(
                            this.psi
                                    .getComparatorWithCurrentReference()
                                    .equalToReference(new Record(new IntValue(3))))
                    .withFailMessage("KeyGroupedIterator returned a wrong key.")
                    .isTrue();
            assertThat(this.psi.getCurrent().getField(0, IntValue.class).getValue())
                    .withFailMessage("KeyGroupedIterator returned a wrong key.")
                    .isEqualTo(3);

            assertThat(this.psi.nextKey())
                    .withFailMessage("KeyGroupedIterator must have another key.")
                    .isTrue();
            assertThat(
                            this.psi
                                    .getComparatorWithCurrentReference()
                                    .equalToReference(new Record(new IntValue(4))))
                    .withFailMessage("KeyGroupedIterator returned a wrong key.")
                    .isTrue();
            assertThat(this.psi.getCurrent().getField(0, IntValue.class).getValue())
                    .withFailMessage("KeyGroupedIterator returned a wrong key.")
                    .isEqualTo(4);

            assertThat(this.psi.nextKey())
                    .withFailMessage("KeyGroupedIterator must have another key.")
                    .isTrue();
            assertThat(
                            this.psi
                                    .getComparatorWithCurrentReference()
                                    .equalToReference(new Record(new IntValue(5))))
                    .withFailMessage("KeyGroupedIterator returned a wrong key.")
                    .isTrue();
            assertThat(this.psi.getCurrent().getField(0, IntValue.class).getValue())
                    .withFailMessage("KeyGroupedIterator returned a wrong key.")
                    .isEqualTo(5);

            assertThat(this.psi.nextKey())
                    .withFailMessage("KeyGroupedIterator must not have another key.")
                    .isFalse();
            assertThat((Iterable<? extends Record>) this.psi.getValues())
                    .withFailMessage("KeyGroupedIterator must not have another value.")
                    .isNull();

            assertThat(this.psi.nextKey())
                    .withFailMessage("KeyGroupedIterator must not have another key.")
                    .isFalse();
            assertThat(this.psi.nextKey())
                    .withFailMessage("KeyGroupedIterator must not have another key.")
                    .isFalse();
        } catch (Exception e) {
            e.printStackTrace();
            fail("The test encountered an unexpected exception.");
        }
    }

    @Test
    void testFullIterationThroughAllValues() throws IOException {
        try {
            // Key 1, Value A
            assertThat(this.psi.nextKey())
                    .withFailMessage("KeyGroupedIterator must have another key.")
                    .isTrue();
            assertThat(hasIterator(this.psi.getValues())).isTrue();
            assertThat(hasIterator(this.psi.getValues())).isFalse();
            assertThat(this.psi.getValues().hasNext())
                    .withFailMessage("KeyGroupedIterator must have another value.")
                    .isTrue();
            assertThat(
                            this.psi
                                    .getComparatorWithCurrentReference()
                                    .equalToReference(new Record(new IntValue(1))))
                    .withFailMessage("KeyGroupedIterator returned a wrong key.")
                    .isTrue();
            assertThat(this.psi.getCurrent().getField(0, IntValue.class).getValue())
                    .withFailMessage("KeyGroupedIterator returned a wrong key.")
                    .isOne();
            assertThat(this.psi.getValues().next().getField(1, StringValue.class).getValue())
                    .withFailMessage("KeyGroupedIterator returned a wrong value.")
                    .isEqualTo("A");
            assertThat(this.psi.getValues().hasNext())
                    .withFailMessage("KeyGroupedIterator must not have another value.")
                    .isFalse();

            // Key 2, Value B
            assertThat(this.psi.nextKey())
                    .withFailMessage("KeyGroupedIterator must have another key.")
                    .isTrue();
            assertThat(hasIterator(this.psi.getValues())).isTrue();
            assertThat(hasIterator(this.psi.getValues())).isFalse();
            assertThat(this.psi.getValues().hasNext())
                    .withFailMessage("KeyGroupedIterator must have another value.")
                    .isTrue();
            assertThat(
                            this.psi
                                    .getComparatorWithCurrentReference()
                                    .equalToReference(new Record(new IntValue(2))))
                    .withFailMessage("KeyGroupedIterator returned a wrong key.")
                    .isTrue();
            assertThat(this.psi.getCurrent().getField(0, IntValue.class).getValue())
                    .withFailMessage("KeyGroupedIterator returned a wrong key.")
                    .isEqualTo(2);
            assertThat(this.psi.getValues().next().getField(1, StringValue.class).getValue())
                    .withFailMessage("KeyGroupedIterator returned a wrong value.")
                    .isEqualTo("B");
            assertThat(this.psi.getValues().hasNext())
                    .withFailMessage("KeyGroupedIterator must not have another value.")
                    .isFalse();

            // Key 3, Values C, D
            assertThat(this.psi.nextKey())
                    .withFailMessage("KeyGroupedIterator must have another key.")
                    .isTrue();
            assertThat(hasIterator(this.psi.getValues())).isTrue();
            assertThat(hasIterator(this.psi.getValues())).isFalse();
            assertThat(this.psi.getValues().hasNext())
                    .withFailMessage("KeyGroupedIterator must have another value.")
                    .isTrue();
            assertThat(
                            this.psi
                                    .getComparatorWithCurrentReference()
                                    .equalToReference(new Record(new IntValue(3))))
                    .withFailMessage("KeyGroupedIterator returned a wrong key.")
                    .isTrue();
            assertThat(this.psi.getCurrent().getField(0, IntValue.class).getValue())
                    .withFailMessage("KeyGroupedIterator returned a wrong key.")
                    .isEqualTo(3);
            assertThat(this.psi.getValues().next().getField(1, StringValue.class).getValue())
                    .withFailMessage("KeyGroupedIterator returned a wrong value.")
                    .isEqualTo("C");
            assertThat(this.psi.getValues().hasNext())
                    .withFailMessage("KeyGroupedIterator must have another value.")
                    .isTrue();
            assertThat(
                            this.psi
                                    .getComparatorWithCurrentReference()
                                    .equalToReference(new Record(new IntValue(3))))
                    .withFailMessage("KeyGroupedIterator returned a wrong key.")
                    .isTrue();
            assertThat(this.psi.getCurrent().getField(0, IntValue.class).getValue())
                    .withFailMessage("KeyGroupedIterator returned a wrong key.")
                    .isEqualTo(3);
            assertThat(this.psi.getValues().next().getField(1, StringValue.class).getValue())
                    .withFailMessage("KeyGroupedIterator returned a wrong value.")
                    .isEqualTo("D");
            assertThat(
                            this.psi
                                    .getComparatorWithCurrentReference()
                                    .equalToReference(new Record(new IntValue(3))))
                    .withFailMessage("KeyGroupedIterator returned a wrong key.")
                    .isTrue();
            assertThat(this.psi.getCurrent().getField(0, IntValue.class).getValue())
                    .withFailMessage("KeyGroupedIterator returned a wrong key.")
                    .isEqualTo(3);
            assertThatThrownBy(() -> this.psi.getValues().next())
                    .withFailMessage(
                            "A new KeyGroupedIterator must not have any value available and hence throw an exception on next().")
                    .isInstanceOf(NoSuchElementException.class);
            assertThat(this.psi.getValues().hasNext())
                    .withFailMessage("KeyGroupedIterator must not have another value.")
                    .isFalse();
            assertThatThrownBy(() -> this.psi.getValues().next())
                    .withFailMessage(
                            "A new KeyGroupedIterator must not have any value available and hence throw an exception on next().")
                    .isInstanceOf(NoSuchElementException.class);
            assertThat(
                            this.psi
                                    .getComparatorWithCurrentReference()
                                    .equalToReference(new Record(new IntValue(3))))
                    .withFailMessage("KeyGroupedIterator returned a wrong key.")
                    .isTrue();
            assertThat(this.psi.getCurrent().getField(0, IntValue.class).getValue())
                    .withFailMessage("KeyGroupedIterator returned a wrong key.")
                    .isEqualTo(3);

            // Key 4, Values E, F, G
            assertThat(this.psi.nextKey())
                    .withFailMessage("KeyGroupedIterator must have another key.")
                    .isTrue();
            assertThat(hasIterator(this.psi.getValues())).isTrue();
            assertThat(hasIterator(this.psi.getValues())).isFalse();
            assertThat(this.psi.getValues().hasNext())
                    .withFailMessage("KeyGroupedIterator must have another value.")
                    .isTrue();
            assertThat(
                            this.psi
                                    .getComparatorWithCurrentReference()
                                    .equalToReference(new Record(new IntValue(4))))
                    .withFailMessage("KeyGroupedIterator returned a wrong key.")
                    .isTrue();
            assertThat(this.psi.getCurrent().getField(0, IntValue.class).getValue())
                    .withFailMessage("KeyGroupedIterator returned a wrong key.")
                    .isEqualTo(4);
            assertThat(this.psi.getValues().next().getField(1, StringValue.class).getValue())
                    .withFailMessage("KeyGroupedIterator returned a wrong value.")
                    .isEqualTo("E");
            assertThat(this.psi.getValues().hasNext())
                    .withFailMessage("KeyGroupedIterator must have another value.")
                    .isTrue();
            assertThat(
                            this.psi
                                    .getComparatorWithCurrentReference()
                                    .equalToReference(new Record(new IntValue(4))))
                    .withFailMessage("KeyGroupedIterator returned a wrong key.")
                    .isTrue();
            assertThat(this.psi.getCurrent().getField(0, IntValue.class).getValue())
                    .withFailMessage("KeyGroupedIterator returned a wrong key.")
                    .isEqualTo(4);
            assertThat(this.psi.getValues().next().getField(1, StringValue.class).getValue())
                    .withFailMessage("KeyGroupedIterator returned a wrong value.")
                    .isEqualTo("F");
            assertThat(
                            this.psi
                                    .getComparatorWithCurrentReference()
                                    .equalToReference(new Record(new IntValue(4))))
                    .withFailMessage("KeyGroupedIterator returned a wrong key.")
                    .isTrue();
            assertThat(this.psi.getCurrent().getField(0, IntValue.class).getValue())
                    .withFailMessage("KeyGroupedIterator returned a wrong key.")
                    .isEqualTo(4);
            assertThat(this.psi.getValues().next().getField(1, StringValue.class).getValue())
                    .withFailMessage("KeyGroupedIterator returned a wrong value.")
                    .isEqualTo("G");
            assertThat(
                            this.psi
                                    .getComparatorWithCurrentReference()
                                    .equalToReference(new Record(new IntValue(4))))
                    .withFailMessage("KeyGroupedIterator returned a wrong key.")
                    .isTrue();
            assertThat(this.psi.getCurrent().getField(0, IntValue.class).getValue())
                    .withFailMessage("KeyGroupedIterator returned a wrong key.")
                    .isEqualTo(4);
            assertThat(this.psi.getValues().hasNext())
                    .withFailMessage("KeyGroupedIterator must not have another value.")
                    .isFalse();
            assertThat(
                            this.psi
                                    .getComparatorWithCurrentReference()
                                    .equalToReference(new Record(new IntValue(4))))
                    .withFailMessage("KeyGroupedIterator returned a wrong key.")
                    .isTrue();
            assertThat(this.psi.getCurrent().getField(0, IntValue.class).getValue())
                    .withFailMessage("KeyGroupedIterator returned a wrong key.")
                    .isEqualTo(4);

            // Key 5, Values H, I, J, K, L
            assertThat(this.psi.nextKey())
                    .withFailMessage("KeyGroupedIterator must have another key.")
                    .isTrue();
            assertThat(hasIterator(this.psi.getValues())).isTrue();
            assertThat(hasIterator(this.psi.getValues())).isFalse();
            assertThat(this.psi.getValues().hasNext())
                    .withFailMessage("KeyGroupedIterator must have another value.")
                    .isTrue();
            assertThat(
                            this.psi
                                    .getComparatorWithCurrentReference()
                                    .equalToReference(new Record(new IntValue(5))))
                    .withFailMessage("KeyGroupedIterator returned a wrong key.")
                    .isTrue();
            assertThat(this.psi.getCurrent().getField(0, IntValue.class).getValue())
                    .withFailMessage("KeyGroupedIterator returned a wrong key.")
                    .isEqualTo(5);
            assertThat(this.psi.getValues().next().getField(1, StringValue.class).getValue())
                    .withFailMessage("KeyGroupedIterator returned a wrong value.")
                    .isEqualTo("H");
            assertThat(this.psi.getValues().hasNext())
                    .withFailMessage("KeyGroupedIterator must have another value.")
                    .isTrue();
            assertThat(
                            this.psi
                                    .getComparatorWithCurrentReference()
                                    .equalToReference(new Record(new IntValue(5))))
                    .withFailMessage("KeyGroupedIterator returned a wrong key.")
                    .isTrue();
            assertThat(this.psi.getCurrent().getField(0, IntValue.class).getValue())
                    .withFailMessage("KeyGroupedIterator returned a wrong key.")
                    .isEqualTo(5);
            assertThat(this.psi.getValues().next().getField(1, StringValue.class).getValue())
                    .withFailMessage("KeyGroupedIterator returned a wrong value.")
                    .isEqualTo("I");
            assertThat(
                            this.psi
                                    .getComparatorWithCurrentReference()
                                    .equalToReference(new Record(new IntValue(5))))
                    .withFailMessage("KeyGroupedIterator returned a wrong key.")
                    .isTrue();
            assertThat(this.psi.getCurrent().getField(0, IntValue.class).getValue())
                    .withFailMessage("KeyGroupedIterator returned a wrong key.")
                    .isEqualTo(5);
            assertThat(this.psi.getValues().next().getField(1, StringValue.class).getValue())
                    .withFailMessage("KeyGroupedIterator returned a wrong value.")
                    .isEqualTo("J");
            assertThat(
                            this.psi
                                    .getComparatorWithCurrentReference()
                                    .equalToReference(new Record(new IntValue(5))))
                    .withFailMessage("KeyGroupedIterator returned a wrong key.")
                    .isTrue();
            assertThat(this.psi.getCurrent().getField(0, IntValue.class).getValue())
                    .withFailMessage("KeyGroupedIterator returned a wrong key.")
                    .isEqualTo(5);
            assertThat(this.psi.getValues().next().getField(1, StringValue.class).getValue())
                    .withFailMessage("KeyGroupedIterator returned a wrong value.")
                    .isEqualTo("K");
            assertThat(
                            this.psi
                                    .getComparatorWithCurrentReference()
                                    .equalToReference(new Record(new IntValue(5))))
                    .withFailMessage("KeyGroupedIterator returned a wrong key.")
                    .isTrue();
            assertThat(this.psi.getCurrent().getField(0, IntValue.class).getValue())
                    .withFailMessage("KeyGroupedIterator returned a wrong key.")
                    .isEqualTo(5);
            assertThat(this.psi.getValues().next().getField(1, StringValue.class).getValue())
                    .withFailMessage("KeyGroupedIterator returned a wrong value.")
                    .isEqualTo("L");
            assertThat(
                            this.psi
                                    .getComparatorWithCurrentReference()
                                    .equalToReference(new Record(new IntValue(5))))
                    .withFailMessage("KeyGroupedIterator returned a wrong key.")
                    .isTrue();
            assertThat(this.psi.getCurrent().getField(0, IntValue.class).getValue())
                    .withFailMessage("KeyGroupedIterator returned a wrong key.")
                    .isEqualTo(5);
            assertThatThrownBy(() -> this.psi.getValues().next())
                    .withFailMessage(
                            "A new KeyGroupedIterator must not have any value available and hence throw an exception on next().")
                    .isInstanceOf(NoSuchElementException.class);
            assertThat(this.psi.getValues().hasNext())
                    .withFailMessage("KeyGroupedIterator must not have another value.")
                    .isFalse();
            assertThat(
                            this.psi
                                    .getComparatorWithCurrentReference()
                                    .equalToReference(new Record(new IntValue(5))))
                    .withFailMessage("KeyGroupedIterator returned a wrong key.")
                    .isTrue();
            assertThat(this.psi.getCurrent().getField(0, IntValue.class).getValue())
                    .withFailMessage("KeyGroupedIterator returned a wrong key.")
                    .isEqualTo(5);
            assertThatThrownBy(() -> this.psi.getValues().next())
                    .withFailMessage(
                            "A new KeyGroupedIterator must not have any value available and hence throw an exception on next().")
                    .isInstanceOf(NoSuchElementException.class);

            assertThat(this.psi.nextKey())
                    .withFailMessage("KeyGroupedIterator must not have another key.")
                    .isFalse();
            assertThat(this.psi.nextKey())
                    .withFailMessage("KeyGroupedIterator must not have another key.")
                    .isFalse();
            assertThat((Iterable<? extends Record>) this.psi.getValues()).isNull();
        } catch (Exception e) {
            e.printStackTrace();
            fail("The test encountered an unexpected exception.");
        }
    }

    @Test
    void testMixedProgress() throws Exception {
        try {
            // Progression only via nextKey() and hasNext() - Key 1, Value A
            assertThat(this.psi.nextKey())
                    .withFailMessage("KeyGroupedIterator must have another key.")
                    .isTrue();
            assertThat(this.psi.getValues().hasNext())
                    .withFailMessage("KeyGroupedIterator must have another value.")
                    .isTrue();
            assertThat(hasIterator(this.psi.getValues())).isTrue();
            assertThat(hasIterator(this.psi.getValues())).isFalse();

            // Progression only through nextKey() - Key 2, Value B
            assertThat(this.psi.nextKey())
                    .withFailMessage("KeyGroupedIterator must have another key.")
                    .isTrue();
            assertThat(hasIterator(this.psi.getValues())).isTrue();
            assertThat(hasIterator(this.psi.getValues())).isFalse();

            // Progression first though haNext() and next(), then through hasNext() - Key 3, Values
            // C, D
            assertThat(this.psi.nextKey())
                    .withFailMessage("KeyGroupedIterator must have another key.")
                    .isTrue();
            assertThat(hasIterator(this.psi.getValues())).isTrue();
            assertThat(hasIterator(this.psi.getValues())).isFalse();
            assertThat(this.psi.getValues().hasNext())
                    .withFailMessage("KeyGroupedIterator must have another value.")
                    .isTrue();
            assertThat(this.psi.getCurrent().getField(0, IntValue.class).getValue())
                    .withFailMessage("KeyGroupedIterator returned a wrong key.")
                    .isEqualTo(3);
            assertThat(this.psi.getCurrent().getField(0, IntValue.class).getValue())
                    .withFailMessage("KeyGroupedIterator returned a wrong key.")
                    .isEqualTo(3);
            assertThat(this.psi.getValues().next().getField(1, StringValue.class).getValue())
                    .withFailMessage("KeyGroupedIterator returned a wrong value.")
                    .isEqualTo("C");
            assertThat(this.psi.getValues().hasNext())
                    .withFailMessage("KeyGroupedIterator must have another value.")
                    .isTrue();
            assertThat(this.psi.getCurrent().getField(0, IntValue.class).getValue())
                    .withFailMessage("KeyGroupedIterator returned a wrong key.")
                    .isEqualTo(3);
            assertThat(this.psi.getCurrent().getField(0, IntValue.class).getValue())
                    .withFailMessage("KeyGroupedIterator returned a wrong key.")
                    .isEqualTo(3);

            // Progression first via next() only, then hasNext() only Key 4, Values E, F, G
            assertThat(this.psi.nextKey())
                    .withFailMessage("KeyGroupedIterator must have another key.")
                    .isTrue();
            assertThat(hasIterator(this.psi.getValues())).isTrue();
            assertThat(hasIterator(this.psi.getValues())).isFalse();
            assertThat(this.psi.getValues().next().getField(1, StringValue.class).getValue())
                    .withFailMessage("KeyGroupedIterator returned a wrong value.")
                    .isEqualTo("E");
            assertThat(this.psi.getValues().hasNext())
                    .withFailMessage("KeyGroupedIterator must have another value.")
                    .isTrue();

            // Key 5, Values H, I, J, K, L
            assertThat(this.psi.nextKey())
                    .withFailMessage("KeyGroupedIterator must have another key.")
                    .isTrue();
            assertThat(this.psi.getValues().next().getField(1, StringValue.class).getValue())
                    .withFailMessage("KeyGroupedIterator returned a wrong value.")
                    .isEqualTo("H");
            assertThat(this.psi.getValues().hasNext())
                    .withFailMessage("KeyGroupedIterator must have another value.")
                    .isTrue();
            assertThat(this.psi.getCurrent().getField(0, IntValue.class).getValue())
                    .withFailMessage("KeyGroupedIterator returned a wrong key.")
                    .isEqualTo(5);
            assertThat(this.psi.getCurrent().getField(0, IntValue.class).getValue())
                    .withFailMessage("KeyGroupedIterator returned a wrong key.")
                    .isEqualTo(5);
            assertThat(this.psi.getValues().next().getField(1, StringValue.class).getValue())
                    .withFailMessage("KeyGroupedIterator returned a wrong value.")
                    .isEqualTo("I");
            assertThat(this.psi.getValues().hasNext())
                    .withFailMessage("KeyGroupedIterator must have another value.")
                    .isTrue();
            assertThat(hasIterator(this.psi.getValues())).isTrue();
            assertThat(hasIterator(this.psi.getValues())).isFalse();
            assertThat(this.psi.getValues().hasNext())
                    .withFailMessage("KeyGroupedIterator must have another value.")
                    .isTrue();

            // end
            assertThat(this.psi.nextKey())
                    .withFailMessage("KeyGroupedIterator must not have another key.")
                    .isFalse();
            assertThat(this.psi.nextKey())
                    .withFailMessage("KeyGroupedIterator must not have another key.")
                    .isFalse();
        } catch (Exception e) {
            e.printStackTrace();
            fail("The test encountered an unexpected exception.");
        }
    }

    @Test
    void testHasNextDoesNotOverwriteCurrentRecord() throws Exception {
        try {
            Iterator<Record> valsIter = null;
            Record rec = null;

            assertThat(this.psi.nextKey())
                    .withFailMessage("KeyGroupedIterator must have another key.")
                    .isTrue();
            valsIter = this.psi.getValues();
            assertThat(valsIter).withFailMessage("Returned Iterator must not be null").isNotNull();
            assertThat(valsIter)
                    .withFailMessage("KeyGroupedIterator's value iterator must have another value.")
                    .hasNext();
            rec = valsIter.next();
            assertThat(rec.getField(0, IntValue.class).getValue())
                    .withFailMessage("KeyGroupedIterator returned a wrong key.")
                    .isOne();
            assertThat(rec.getField(1, StringValue.class).getValue())
                    .withFailMessage("KeyGroupedIterator returned a wrong value.")
                    .isEqualTo("A");
            assertThat(valsIter)
                    .withFailMessage("KeyGroupedIterator must not have another value.")
                    .isExhausted();
            assertThat(rec.getField(0, IntValue.class).getValue())
                    .withFailMessage("KeyGroupedIterator returned a wrong key.")
                    .isOne();
            assertThat(rec.getField(1, StringValue.class).getValue())
                    .withFailMessage("KeyGroupedIterator returned a wrong value.")
                    .isEqualTo("A");
            assertThat(valsIter)
                    .withFailMessage(
                            "KeyGroupedIterator's value iterator must not have another value.")
                    .isExhausted();

            assertThat(this.psi.nextKey())
                    .withFailMessage("KeyGroupedIterator must have another key.")
                    .isTrue();
            valsIter = this.psi.getValues();
            assertThat(valsIter).withFailMessage("Returned Iterator must not be null").isNotNull();
            assertThat(valsIter)
                    .withFailMessage("KeyGroupedIterator's value iterator must have another value.")
                    .hasNext();
            rec = valsIter.next();
            assertThat(rec.getField(0, IntValue.class).getValue())
                    .withFailMessage("KeyGroupedIterator returned a wrong key.")
                    .isEqualTo(2);
            assertThat(rec.getField(1, StringValue.class).getValue())
                    .withFailMessage("KeyGroupedIterator returned a wrong value.")
                    .isEqualTo("B");
            assertThat(valsIter)
                    .withFailMessage("KeyGroupedIterator must not have another value.")
                    .isExhausted();
            assertThat(rec.getField(0, IntValue.class).getValue())
                    .withFailMessage("KeyGroupedIterator returned a wrong key.")
                    .isEqualTo(2);
            assertThat(rec.getField(1, StringValue.class).getValue())
                    .withFailMessage("KeyGroupedIterator returned a wrong value.")
                    .isEqualTo("B");
            assertThat(valsIter)
                    .withFailMessage(
                            "KeyGroupedIterator's value iterator must not have another value.")
                    .isExhausted();

            assertThat(this.psi.nextKey())
                    .withFailMessage("KeyGroupedIterator must have another key.")
                    .isTrue();
            valsIter = this.psi.getValues();
            assertThat(valsIter).withFailMessage("Returned Iterator must not be null").isNotNull();
            assertThat(valsIter)
                    .withFailMessage("KeyGroupedIterator's value iterator must have another value.")
                    .hasNext();
            rec = valsIter.next();
            assertThat(rec.getField(0, IntValue.class).getValue())
                    .withFailMessage("KeyGroupedIterator returned a wrong key.")
                    .isEqualTo(3);
            assertThat(rec.getField(1, StringValue.class).getValue())
                    .withFailMessage("KeyGroupedIterator returned a wrong value.")
                    .isEqualTo("C");
            assertThat(valsIter)
                    .withFailMessage("KeyGroupedIterator's value iterator must have another value.")
                    .hasNext();
            assertThat(rec.getField(0, IntValue.class).getValue())
                    .withFailMessage("KeyGroupedIterator returned a wrong key.")
                    .isEqualTo(3);
            assertThat(rec.getField(1, StringValue.class).getValue())
                    .withFailMessage("KeyGroupedIterator returned a wrong value.")
                    .isEqualTo("C");
            rec = valsIter.next();
            assertThat(rec.getField(0, IntValue.class).getValue())
                    .withFailMessage("KeyGroupedIterator returned a wrong key.")
                    .isEqualTo(3);
            assertThat(rec.getField(1, StringValue.class).getValue())
                    .withFailMessage("KeyGroupedIterator returned a wrong value.")
                    .isEqualTo("D");
            assertThat(valsIter)
                    .withFailMessage("KeyGroupedIterator must not have another value.")
                    .isExhausted();
            assertThat(rec.getField(0, IntValue.class).getValue())
                    .withFailMessage("KeyGroupedIterator returned a wrong key.")
                    .isEqualTo(3);
            assertThat(rec.getField(1, StringValue.class).getValue())
                    .withFailMessage("KeyGroupedIterator returned a wrong value.")
                    .isEqualTo("D");
            assertThat(valsIter)
                    .withFailMessage(
                            "KeyGroupedIterator's value iterator must not have another value.")
                    .isExhausted();
            assertThat(rec.getField(0, IntValue.class).getValue())
                    .withFailMessage("KeyGroupedIterator returned a wrong key.")
                    .isEqualTo(3);
            assertThat(rec.getField(1, StringValue.class).getValue())
                    .withFailMessage("KeyGroupedIterator returned a wrong value.")
                    .isEqualTo("D");
        } catch (Exception e) {
            e.printStackTrace();
            fail("The test encountered an unexpected exception.");
        }
    }

    private static final class IntStringPair {
        private final IntValue integer;
        private final StringValue string;

        IntStringPair(IntValue integer, StringValue string) {
            this.integer = integer;
            this.string = string;
        }

        public IntValue getInteger() {
            return integer;
        }

        public StringValue getString() {
            return string;
        }
    }

    public boolean hasIterator(Iterable<?> iterable) {
        try {
            iterable.iterator();
            return true;
        } catch (TraversableOnceException e) {
            return false;
        }
    }
}
