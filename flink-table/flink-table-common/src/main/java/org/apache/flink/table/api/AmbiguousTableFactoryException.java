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

package org.apache.flink.table.api;

import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.TableFactory;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Exception for finding more than one {@link TableFactory} for the given properties. */
public class AmbiguousTableFactoryException extends RuntimeException {

    // factories that match the properties
    private final List<? extends TableFactory> matchingFactories;
    // required factory class
    private final Class<? extends TableFactory> factoryClass;
    // all found factories
    private final List<TableFactory> factories;
    // properties that describe the configuration
    private final Map<String, String> properties;

    public AmbiguousTableFactoryException(
            List<? extends TableFactory> matchingFactories,
            Class<? extends TableFactory> factoryClass,
            List<TableFactory> factories,
            Map<String, String> properties,
            Throwable cause) {

        super(cause);
        this.matchingFactories = matchingFactories;
        this.factoryClass = factoryClass;
        this.factories = factories;
        this.properties = properties;
    }

    public AmbiguousTableFactoryException(
            List<? extends TableFactory> matchingFactories,
            Class<? extends TableFactory> factoryClass,
            List<TableFactory> factories,
            Map<String, String> properties) {

        this(matchingFactories, factoryClass, factories, properties, null);
    }

    @Override
    public String getMessage() {
        return String.format(
                "More than one suitable table factory for '%s' could be found in the classpath.\n\n"
                        + "The following factories match:\n%s\n\n"
                        + "The following properties are requested:\n%s\n\n"
                        + "The following factories have been considered:\n%s",
                factoryClass.getName(),
                matchingFactories.stream()
                        .map(p -> p.getClass().getName())
                        .collect(Collectors.joining("\n")),
                DescriptorProperties.toString(properties),
                factories.stream()
                        .map(p -> p.getClass().getName())
                        .collect(Collectors.joining("\n")));
    }
}
