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

package org.apache.flink.connector.mongodb.source.enumerator.splitter;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.DescribedEnum;
import org.apache.flink.configuration.description.InlineElement;

import static org.apache.flink.configuration.description.TextElement.text;

/** MongoSplitStrategy that can be chosen. */
@PublicEvolving
public enum PartitionStrategy implements DescribedEnum {
    SINGLE("single", text("Do not split, treat a collection as a single chunk.")),

    SAMPLE("sample", text("Randomly sample the collection, then splits to multiple chunks.")),

    SPLIT_VECTOR(
            "split-vector",
            text("Uses the SplitVector command to generate chunks for a collection.")),

    SHARDED(
            "sharded",
            text(
                    "Read the chunk ranges from config.chunks collection and splits to multiple chunks. Only support sharded collections.")),

    DEFAULT(
            "default",
            text(
                    "Using sharded strategy for sharded collections"
                            + " otherwise using split vector strategy."));

    private final String name;
    private final InlineElement description;

    PartitionStrategy(String name, InlineElement description) {
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
