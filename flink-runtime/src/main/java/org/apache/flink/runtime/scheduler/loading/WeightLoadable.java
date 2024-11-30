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

import javax.annotation.Nonnull;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * The interface that holds the {@link LoadingWeight} getter is required for corresponding
 * abstractions.
 */
@Internal
public interface WeightLoadable {

    /**
     * Get the loading weight.
     *
     * @return An implementation object of {@link LoadingWeight}.
     */
    @Nonnull
    LoadingWeight getLoading();

    static <T extends WeightLoadable> List<T> sortByLoadingDescend(Collection<T> weightLoadables) {
        return weightLoadables.stream()
                .sorted(
                        (leftReq, rightReq) ->
                                rightReq.getLoading().compareTo(leftReq.getLoading()))
                .collect(Collectors.toList());
    }
}
