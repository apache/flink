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

package org.apache.flink.runtime.scheduler.loading;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/** The class is used to represent the loading weight abstraction of slots. */
@Internal
public interface LoadingWeight extends Comparable<LoadingWeight>, Serializable {

    LoadingWeight EMPTY = new DefaultLoadingWeight(0f);

    static LoadingWeight ofDefaultLoadingWeight(float loading) {
        return new DefaultLoadingWeight(loading);
    }

    static List<LoadingWeight> ofDefaultLoadingWeights(int... loadings) {
        List<LoadingWeight> loadingWeights = new ArrayList<>(loadings.length);
        for (int loading : loadings) {
            loadingWeights.add(ofDefaultLoadingWeight(loading));
        }
        return loadingWeights;
    }

    static List<LoadingWeight> supplyEmptyLoadWeights(int number) {
        Preconditions.checkArgument(number >= 0);
        LoadingWeight[] loadingWeights = new LoadingWeight[number];
        Arrays.parallelSetAll(loadingWeights, value -> EMPTY);
        return Arrays.stream(loadingWeights).collect(Collectors.toList());
    }

    /**
     * Get the loading value.
     *
     * @return A float represented the loading.
     */
    float getLoading();

    /**
     * Merge the other loading weight into itself.
     *
     * @param other A loading weight object.
     * @return The new merged {@link LoadingWeight}.
     */
    LoadingWeight merge(LoadingWeight other);
}
