/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.TestLogger;

import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;

/** Abstract test base for implementations of {@link KeyGroupPartitioner}. */
public abstract class KeyGroupPartitionerTestBase<T> extends TestLogger {

    private static final DataOutputView DUMMY_OUT_VIEW =
            new DataOutputViewStreamWrapper(new ByteArrayOutputStreamWithPos(0));

    @Nonnull protected final KeyExtractorFunction<T> keyExtractorFunction;

    @Nonnull protected final Function<Random, T> elementGenerator;

    protected KeyGroupPartitionerTestBase(
            @Nonnull Function<Random, T> elementGenerator,
            @Nonnull KeyExtractorFunction<T> keyExtractorFunction) {

        this.elementGenerator = elementGenerator;
        this.keyExtractorFunction = keyExtractorFunction;
    }

    @Test
    public void testPartitionByKeyGroup() throws IOException {

        final Random random = new Random(0x42);
        testPartitionByKeyGroupForSize(0, random);
        testPartitionByKeyGroupForSize(1, random);
        testPartitionByKeyGroupForSize(2, random);
        testPartitionByKeyGroupForSize(10, random);
    }

    private void testPartitionByKeyGroupForSize(int testSize, Random random) throws IOException {

        final Set<T> allElementsIdentitySet = Collections.newSetFromMap(new IdentityHashMap<>());
        final T[] data = generateTestInput(random, testSize, allElementsIdentitySet);

        Assert.assertEquals(testSize, allElementsIdentitySet.size());

        // Test with 5 key-groups.
        final KeyGroupRange range = new KeyGroupRange(0, 4);
        final int numberOfKeyGroups = range.getNumberOfKeyGroups();

        final ValidatingElementWriterDummy<T> validatingElementWriter =
                new ValidatingElementWriterDummy<>(
                        keyExtractorFunction, numberOfKeyGroups, allElementsIdentitySet);

        final KeyGroupPartitioner<T> testInstance =
                createPartitioner(
                        data, testSize, range, numberOfKeyGroups, validatingElementWriter);
        final StateSnapshot.StateKeyGroupWriter result = testInstance.partitionByKeyGroup();

        for (int keyGroup = 0; keyGroup < numberOfKeyGroups; ++keyGroup) {
            validatingElementWriter.setCurrentKeyGroup(keyGroup);
            result.writeStateInKeyGroup(DUMMY_OUT_VIEW, keyGroup);
        }

        validatingElementWriter.validateAllElementsSeen();
    }

    @Test
    public void testPartitionByKeyGroupWithIterator() throws IOException {

        final Random random = new Random(0x42);
        testPartitionByKeyGroupForSizeWithIterator(0, random);
        testPartitionByKeyGroupForSizeWithIterator(1, random);
        testPartitionByKeyGroupForSizeWithIterator(2, random);
        testPartitionByKeyGroupForSizeWithIterator(10, random);
    }

    private void testPartitionByKeyGroupForSizeWithIterator(int testSize, Random random) {
        // Test with 5 key-groups.
        final KeyGroupRange range = new KeyGroupRange(0, 4);
        final int numberOfKeyGroups = range.getNumberOfKeyGroups();

        final Set<T> allElementsIdentitySet = Collections.newSetFromMap(new IdentityHashMap<>());
        final T[] data = generateTestInput(random, testSize, allElementsIdentitySet);
        Map<Integer, List<T>> partitionedData =
                Arrays.stream(data)
                        .filter(Objects::nonNull)
                        .collect(
                                Collectors.toMap(
                                        el -> computeKeyGroup(numberOfKeyGroups, el),
                                        Arrays::asList,
                                        (l1, l2) -> {
                                            List<T> mergedList = new ArrayList<>(l1);
                                            mergedList.addAll(l2);
                                            return mergedList;
                                        }));

        final KeyGroupPartitioner<T> testInstance =
                createPartitioner(
                        data, testSize, range, numberOfKeyGroups, new NoOpElementWriter<>());
        final KeyGroupPartitioner.PartitioningResult<T> result = testInstance.partitionByKeyGroup();

        for (int keyGroup = 0; keyGroup < numberOfKeyGroups; ++keyGroup) {
            Iterator<T> iterator = result.iterator(keyGroup);
            assertThat(
                    CollectionUtil.iteratorToList(iterator),
                    containsInAnyOrder(
                            partitionedData.getOrDefault(keyGroup, Collections.emptyList()).stream()
                                    .map(Matchers::equalTo)
                                    .collect(Collectors.toList())));
        }
    }

    private int computeKeyGroup(int numberOfKeyGroups, T el) {
        return KeyGroupRangeAssignment.assignToKeyGroup(
                keyExtractorFunction.extractKeyFromElement(el), numberOfKeyGroups);
    }

    @SuppressWarnings("unchecked")
    protected T[] generateTestInput(
            Random random, int numElementsToGenerate, Set<T> allElementsIdentitySet) {

        final int arraySize =
                numElementsToGenerate > 1 ? numElementsToGenerate + 5 : numElementsToGenerate;
        T element = elementGenerator.apply(random);
        final T[] partitioningIn = (T[]) Array.newInstance(element.getClass(), arraySize);

        for (int i = 0; i < numElementsToGenerate; ++i) {
            partitioningIn[i] = element;
            allElementsIdentitySet.add(element);
            element = elementGenerator.apply(random);
        }

        Assert.assertEquals(numElementsToGenerate, allElementsIdentitySet.size());
        return partitioningIn;
    }

    @SuppressWarnings("unchecked")
    protected KeyGroupPartitioner<T> createPartitioner(
            T[] data,
            int numElements,
            KeyGroupRange keyGroupRange,
            int totalKeyGroups,
            KeyGroupPartitioner.ElementWriterFunction<T> elementWriterFunction) {

        final T[] partitioningOut =
                (T[]) Array.newInstance(data.getClass().getComponentType(), numElements);
        return new KeyGroupPartitioner<>(
                data,
                numElements,
                partitioningOut,
                keyGroupRange,
                totalKeyGroups,
                keyExtractorFunction,
                elementWriterFunction);
    }

    static final class NoOpElementWriter<T>
            implements KeyGroupPartitioner.ElementWriterFunction<T> {
        @Override
        public void writeElement(@Nonnull T element, @Nonnull DataOutputView dov)
                throws IOException {}
    }

    /** Simple test implementation with validation . */
    static final class ValidatingElementWriterDummy<T>
            implements KeyGroupPartitioner.ElementWriterFunction<T> {

        @Nonnull private final KeyExtractorFunction<T> keyExtractorFunction;
        @Nonnegative private final int numberOfKeyGroups;
        @Nonnull private final Set<T> allElementsSet;
        @Nonnegative private int currentKeyGroup;

        ValidatingElementWriterDummy(
                @Nonnull KeyExtractorFunction<T> keyExtractorFunction,
                @Nonnegative int numberOfKeyGroups,
                @Nonnull Set<T> allElementsSet) {
            this.keyExtractorFunction = keyExtractorFunction;
            this.numberOfKeyGroups = numberOfKeyGroups;
            this.allElementsSet = allElementsSet;
        }

        @Override
        public void writeElement(@Nonnull T element, @Nonnull DataOutputView dov) {
            Assert.assertTrue(allElementsSet.remove(element));
            Assert.assertEquals(
                    currentKeyGroup,
                    KeyGroupRangeAssignment.assignToKeyGroup(
                            keyExtractorFunction.extractKeyFromElement(element),
                            numberOfKeyGroups));
        }

        void validateAllElementsSeen() {
            Assert.assertTrue(allElementsSet.isEmpty());
        }

        void setCurrentKeyGroup(int currentKeyGroup) {
            this.currentKeyGroup = currentKeyGroup;
        }
    }
}
