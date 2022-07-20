/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.base;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.DescribedEnum;
import org.apache.flink.configuration.description.InlineElement;

import static org.apache.flink.configuration.description.TextElement.text;

/**
 * DeliverGuarantees that can be chosen. In general your pipeline can only offer the lowest delivery
 * guarantee which is supported by your sources and sinks.
 */
@PublicEvolving
public enum DeliveryGuarantee implements DescribedEnum {
    /**
     * Records are only delivered exactly-once also under failover scenarios. To build a complete
     * exactly-once pipeline is required that the source and sink support exactly-once and are
     * properly configured.
     */
    EXACTLY_ONCE(
            "exactly-once",
            text(
                    "Records are only delivered exactly-once also under failover scenarios. To build a complete exactly-once pipeline is required that the source and sink support exactly-once and are properly configured.")),
    /**
     * Records are ensured to be delivered but it may happen that the same record is delivered
     * multiple times. Usually, this guarantee is faster than the exactly-once delivery.
     */
    AT_LEAST_ONCE(
            "at-least-once",
            text(
                    "Records are ensured to be delivered but it may happen that the same record is delivered multiple times. Usually, this guarantee is faster than the exactly-once delivery.")),
    /**
     * Records are delivered on a best effort basis. It is often the fastest way to process records
     * but it may happen that records are lost or duplicated.
     */
    NONE(
            "none",
            text(
                    "Records are delivered on a best effort basis. It is often the fastest way to process records but it may happen that records are lost or duplicated."));

    private final String name;
    private final InlineElement description;

    DeliveryGuarantee(String name, InlineElement description) {
        this.name = name;
        this.description = description;
    }

    @Override
    public InlineElement getDescription() {
        return description;
    }

    @Override
    public String toString() {
        return name;
    }
}
