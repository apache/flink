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

package org.apache.flink.connector.base.sink.writer;

import java.io.Serializable;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Utils class for {@link AsyncSinkWriter} related test. */
public class AsyncSinkWriterTestUtils {

    public static <T extends Serializable> BufferedRequestState<T> getTestState(
            ElementConverter<String, T> elementConverter,
            Function<T, Integer> requestSizeExtractor) {
        return new BufferedRequestState<>(
                IntStream.range(0, 100)
                        .mapToObj(i -> String.format("value:%d", i))
                        .map(element -> elementConverter.apply(element, null))
                        .map(
                                request ->
                                        new RequestEntryWrapper<>(
                                                request, requestSizeExtractor.apply(request)))
                        .collect(Collectors.toList()));
    }

    public static <T extends Serializable> void assertThatBufferStatesAreEqual(
            BufferedRequestState<T> actual, BufferedRequestState<T> expected) {
        // Equal states must have equal sizes
        assertEquals(actual.getStateSize(), expected.getStateSize());

        // Equal states must have the same number of requests.
        int actualLength = actual.getBufferedRequestEntries().size();
        assertEquals(actualLength, expected.getBufferedRequestEntries().size());

        List<RequestEntryWrapper<T>> actualRequests = actual.getBufferedRequestEntries();
        List<RequestEntryWrapper<T>> expectedRequests = expected.getBufferedRequestEntries();

        // Equal states must have same requests in the same order.
        for (int i = 0; i < actualLength; i++) {
            assertEquals(
                    actualRequests.get(i).getRequestEntry(),
                    expectedRequests.get(i).getRequestEntry());
            assertEquals(actualRequests.get(i).getSize(), expectedRequests.get(i).getSize());
        }
    }
}
