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

package org.apache.flink.runtime.io;

import org.apache.flink.annotation.Internal;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/** Reports available only when all child {@link AvailabilityProvider}s are available. */
@Internal
public class CompositeAvailabilityProvider implements AvailabilityProvider {
    private final List<AvailabilityProvider> providers;

    private CompositeAvailabilityProvider(List<AvailabilityProvider> providers) {
        this.providers = new ArrayList<>(providers);
    }

    @Override
    public CompletableFuture<?> getAvailableFuture() {
        CompletableFuture<?> result = AvailabilityProvider.AVAILABLE;
        for (AvailabilityProvider p : providers) {
            result = AvailabilityProvider.and(result, p.getAvailableFuture());
        }
        return result;
    }

    public static AvailabilityProvider of(List<AvailabilityProvider> providers) {
        if (providers.size() == 1) {
            return providers.get(0);
        }
        return new CompositeAvailabilityProvider(providers);
    }
}
