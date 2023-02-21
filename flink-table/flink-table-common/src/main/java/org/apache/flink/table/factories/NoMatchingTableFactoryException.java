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

package org.apache.flink.table.factories;

import org.apache.flink.annotation.Internal;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;

/**
 * Exception for not finding a {@link TableFactory} for the given properties.
 *
 * <p>This replaces the deprecated {@link
 * org.apache.flink.table.api.NoMatchingTableFactoryException}.
 */
@Internal
public class NoMatchingTableFactoryException
        extends org.apache.flink.table.api.NoMatchingTableFactoryException {

    public NoMatchingTableFactoryException(
            String message,
            @Nullable String matchCandidatesMessage,
            Class<?> factoryClass,
            List<TableFactory> factories,
            Map<String, String> properties,
            Throwable cause) {
        super(message, matchCandidatesMessage, factoryClass, factories, properties, cause);
    }

    public NoMatchingTableFactoryException(
            String message,
            Class<?> factoryClass,
            List<TableFactory> factories,
            Map<String, String> properties) {
        super(message, factoryClass, factories, properties);
    }

    public NoMatchingTableFactoryException(
            String message,
            @Nullable String matchCandidatesMessage,
            Class<?> factoryClass,
            List<TableFactory> factories,
            Map<String, String> properties) {
        super(message, matchCandidatesMessage, factoryClass, factories, properties);
    }
}
