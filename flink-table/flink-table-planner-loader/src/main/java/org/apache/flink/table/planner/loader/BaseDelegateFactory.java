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

package org.apache.flink.table.planner.loader;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.factories.Factory;

import java.util.Set;

/** Base class for all factory delegates. */
abstract class BaseDelegateFactory<DELEGATE extends Factory> implements Factory {

    final DELEGATE delegate;

    protected BaseDelegateFactory(DELEGATE delegate) {
        this.delegate = delegate;
    }

    @Override
    public String factoryIdentifier() {
        return delegate.factoryIdentifier();
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return delegate.requiredOptions();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return delegate.optionalOptions();
    }
}
