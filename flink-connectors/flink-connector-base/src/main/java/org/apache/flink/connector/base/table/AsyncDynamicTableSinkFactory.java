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

package org.apache.flink.connector.base.table;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.DynamicTableSinkFactory;

import java.util.HashSet;
import java.util.Set;

/**
 * Abstract Implementation of {@link DynamicTableSinkFactory} having {@link
 * org.apache.flink.connector.base.sink.AsyncSinkBase} fields as optional table options defined in
 * {@link AsyncSinkConnectorOptions}.
 */
@PublicEvolving
public abstract class AsyncDynamicTableSinkFactory implements DynamicTableFactory {

    @Override
    public Set<ConfigOption<?>> optionalOptions() {

        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(AsyncSinkConnectorOptions.MAX_BATCH_SIZE);
        options.add(AsyncSinkConnectorOptions.FLUSH_BUFFER_SIZE);
        options.add(AsyncSinkConnectorOptions.MAX_BUFFERED_REQUESTS);
        options.add(AsyncSinkConnectorOptions.FLUSH_BUFFER_TIMEOUT);
        options.add(AsyncSinkConnectorOptions.MAX_IN_FLIGHT_REQUESTS);
        return options;
    }
}
