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

package org.apache.flink.table.client.config;

import org.apache.flink.configuration.DescribedEnum;
import org.apache.flink.configuration.description.InlineElement;

import static org.apache.flink.configuration.description.TextElement.text;

/** The mode when display the result of the query in the sql client. */
public enum ResultMode implements DescribedEnum {
    TABLE(
            text(
                    "Materializes results in memory and visualizes them in a regular, paginated table representation.")),

    CHANGELOG(text("Visualizes the result stream that is produced by a continuous query.")),

    TABLEAU(text("Display results in the screen directly in a tableau format."));

    private final InlineElement description;

    ResultMode(InlineElement description) {
        this.description = description;
    }

    @Override
    public InlineElement getDescription() {
        return description;
    }
}
