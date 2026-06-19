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

package org.apache.flink.table.catalog;

import org.apache.flink.annotation.Internal;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** A catalog connection implementation. */
@Internal
public class DefaultCatalogConnection implements CatalogConnection {

    private final Map<String, String> options;
    private final @Nullable String comment;

    protected DefaultCatalogConnection(Map<String, String> options, @Nullable String comment) {
        this.options = checkNotNull(options, "Options must not be null.");
        this.comment = comment;
    }

    @Override
    public Map<String, String> getOptions() {
        return options;
    }

    @Override
    public String getComment() {
        return comment;
    }

    @Override
    public CatalogConnection copy() {
        return new DefaultCatalogConnection(this.options, this.comment);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DefaultCatalogConnection that = (DefaultCatalogConnection) o;
        return options.equals(that.options) && Objects.equals(comment, that.comment);
    }

    @Override
    public int hashCode() {
        return Objects.hash(options, comment);
    }

    @Override
    public String toString() {
        return "DefaultCatalogConnection{" + "options=" + options + ", comment=" + comment + "}";
    }
}
