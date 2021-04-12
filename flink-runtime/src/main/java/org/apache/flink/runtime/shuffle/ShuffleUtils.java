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

package org.apache.flink.runtime.shuffle;

import java.util.function.Function;

/** Common utility methods for shuffle service. */
public class ShuffleUtils {

    private ShuffleUtils() {}

    /**
     * Apply different functions to known and unknown {@link ShuffleDescriptor}s.
     *
     * <p>Also casts known {@link ShuffleDescriptor}.
     *
     * @param shuffleDescriptorClass concrete class of {@code shuffleDescriptor}
     * @param shuffleDescriptor concrete shuffle descriptor to check
     * @param functionOfUnknownDescriptor supplier to call in case {@code shuffleDescriptor} is
     *     unknown
     * @param functionOfKnownDescriptor function to call in case {@code shuffleDescriptor} is known
     * @param <T> return type of called functions
     * @param <SD> concrete type of {@code shuffleDescriptor} to check
     * @return result of either function call
     */
    @SuppressWarnings("unchecked")
    public static <T, SD extends ShuffleDescriptor> T applyWithShuffleTypeCheck(
            Class<SD> shuffleDescriptorClass,
            ShuffleDescriptor shuffleDescriptor,
            Function<UnknownShuffleDescriptor, T> functionOfUnknownDescriptor,
            Function<SD, T> functionOfKnownDescriptor) {
        if (shuffleDescriptor.isUnknown()) {
            return functionOfUnknownDescriptor.apply((UnknownShuffleDescriptor) shuffleDescriptor);
        } else if (shuffleDescriptorClass.equals(shuffleDescriptor.getClass())) {
            return functionOfKnownDescriptor.apply((SD) shuffleDescriptor);
        } else {
            throw new IllegalArgumentException(
                    String.format(
                            "Unsupported ShuffleDescriptor type <%s>, only <%s> is supported",
                            shuffleDescriptor.getClass().getName(),
                            shuffleDescriptorClass.getName()));
        }
    }
}
