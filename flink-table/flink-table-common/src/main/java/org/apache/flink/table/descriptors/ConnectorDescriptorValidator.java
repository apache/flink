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

package org.apache.flink.table.descriptors;

import org.apache.flink.annotation.Internal;

/** Base validator for options of a connector. */
@Internal
@Deprecated
public abstract class ConnectorDescriptorValidator implements DescriptorValidator {

    /** Prefix for connector-related properties. */
    public static final String CONNECTOR = "connector";

    /** Key for describing the type of the connector. Usually used for factory discovery. */
    public static final String CONNECTOR_TYPE = "connector.type";

    /**
     * Key for describing the property version. This property can be used for backwards
     * compatibility in case the property format changes.
     */
    public static final String CONNECTOR_PROPERTY_VERSION = "connector.property-version";

    /**
     * Key for describing the version of the connector. This property can be used for different
     * connector versions (e.g. Kafka 0.10 or Kafka 0.11).
     */
    public static final String CONNECTOR_VERSION = "connector.version";

    @Override
    public void validate(DescriptorProperties properties) {
        properties.validateString(CONNECTOR_TYPE, false, 1);
        properties.validateInt(CONNECTOR_PROPERTY_VERSION, true, 0);
    }
}
