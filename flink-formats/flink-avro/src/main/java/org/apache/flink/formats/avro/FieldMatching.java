/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.formats.avro;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.DescribedEnum;
import org.apache.flink.configuration.description.InlineElement;
import org.apache.flink.table.types.logical.RowType;

import static org.apache.flink.configuration.description.TextElement.text;

/**
 * Strategy used to pair the fields of a Flink {@link RowType} with the fields of an Avro record
 * schema.
 *
 * <p>{@link #INDEX} is the historical - and still the default - behaviour. It is the right choice
 * whenever the Avro schema is derived from the row type, because then both sides are guaranteed to
 * agree on field order. {@link #NAME} exists for the case where the Avro schema is supplied
 * independently of the table schema, for instance by a schema registry, where the two orders need
 * not agree.
 */
@PublicEvolving
public enum FieldMatching implements DescribedEnum {
    INDEX(
            "index",
            text(
                    "Pair the n-th row field with the n-th Avro field. Requires both schemas to "
                            + "declare their fields in the same order.")),
    NAME(
            "name",
            text(
                    "Pair fields by name, so that field order may differ between the row type and "
                            + "the Avro schema. Names are compared exactly first, then against Avro "
                            + "field aliases, and finally ignoring case."));

    private final String value;
    private final InlineElement description;

    FieldMatching(String value, InlineElement description) {
        this.value = value;
        this.description = description;
    }

    @Override
    public String toString() {
        return value;
    }

    @Override
    public InlineElement getDescription() {
        return description;
    }
}
