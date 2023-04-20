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

package org.apache.flink.runtime.resourcemanager.slotmanager;

import org.apache.flink.runtime.clusterframework.types.ResourceID;

import javax.annotation.Nonnull;

import java.util.Collection;
import java.util.function.Consumer;
import java.util.function.Supplier;

/** Testing implementation of the {@link ResourceAllocator}. */
public class TestingResourceAllocator implements ResourceAllocator {

    @Nonnull private final Consumer<Collection<ResourceDeclaration>> declareResourceNeededConsumer;
    @Nonnull private final Supplier<Boolean> isSupportedSupplier;

    public TestingResourceAllocator(
            @Nonnull Consumer<Collection<ResourceDeclaration>> declareResourceNeededConsumer,
            @Nonnull Supplier<Boolean> isSupportedSupplier) {
        this.declareResourceNeededConsumer = declareResourceNeededConsumer;
        this.isSupportedSupplier = isSupportedSupplier;
    }

    @Override
    public boolean isSupported() {
        return isSupportedSupplier.get();
    }

    @Override
    public void cleaningUpDisconnectedResource(ResourceID resourceID) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void declareResourceNeeded(Collection<ResourceDeclaration> resourceDeclarations) {
        declareResourceNeededConsumer.accept(resourceDeclarations);
    }
}
