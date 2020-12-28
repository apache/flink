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

package org.apache.flink.configuration.description;

import java.util.Arrays;
import java.util.List;

/** Represents a list in the {@link Description}. */
public class ListElement implements BlockElement {

    private final List<InlineElement> entries;

    /**
     * Creates a list with blocks of text. For example:
     *
     * <pre>{@code
     * .list(
     * 	text("this is first element of list"),
     * 	text("this is second element of list with a %s", link("https://link"))
     * )
     * }</pre>
     *
     * @param elements list of this list entries
     * @return list representation
     */
    public static ListElement list(InlineElement... elements) {
        return new ListElement(Arrays.asList(elements));
    }

    public List<InlineElement> getEntries() {
        return entries;
    }

    private ListElement(List<InlineElement> entries) {
        this.entries = entries;
    }

    @Override
    public void format(Formatter formatter) {
        formatter.format(this);
    }
}
