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

package org.apache.flink.table.test.lookup.cache;

import org.apache.flink.table.connector.source.lookup.cache.LookupCache;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import org.assertj.core.api.AbstractAssert;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

/** Assertions for validating status of {@link LookupCache}. */
public class LookupCacheAssert extends AbstractAssert<LookupCacheAssert, LookupCache> {

    public static LookupCacheAssert assertThat(LookupCache actual) {
        return new LookupCacheAssert(actual);
    }

    public LookupCacheAssert(LookupCache actual) {
        super(actual, LookupCacheAssert.class);
    }

    public LookupCacheAssert hasSize(int size) {
        if (actual.size() != size) {
            failWithMessage(
                    "Expected lookup cache to have %d entries but was %d", size, actual.size());
        }
        return this;
    }

    public LookupCacheAssert containsKey(RowData keyRow) {
        if (actual.getIfPresent(keyRow) == null) {
            failWithMessage("Expected lookup cache to contain key '%s' but not found", keyRow);
        }
        return this;
    }

    public LookupCacheAssert containsKey(Object... keyFields) {
        return containsKey(GenericRowData.of(keyFields));
    }

    public LookupCacheAssert doesNotContainKey(RowData keyRow) {
        if (actual.getIfPresent(keyRow) != null) {
            failWithMessage("Expected lookup cache not to contain key '%s' but found");
        }
        return this;
    }

    public LookupCacheAssert doesNotContainKey(Object... keyFields) {
        return doesNotContainKey(GenericRowData.of(keyFields));
    }

    public LookupCacheAssert containsExactlyEntriesOf(Map<RowData, Collection<RowData>> entries) {
        hasSize(entries.size());
        for (Map.Entry<RowData, Collection<RowData>> entry : entries.entrySet()) {
            contains(entry.getKey(), entry.getValue());
        }
        return this;
    }

    public LookupCacheAssert contains(RowData keyRow, Collection<RowData> valueRows) {
        containsKey(keyRow);
        Collection<RowData> cachedValueRows = actual.getIfPresent(keyRow);
        assert cachedValueRows != null;
        if (!cachedValueRows.equals(valueRows)) {
            failWithActualExpectedAndMessage(
                    actual.getIfPresent(keyRow),
                    valueRows,
                    "Lookup cache entry with key '%s' is not as expected",
                    keyRow);
        }
        return this;
    }

    public LookupCacheAssert contains(RowData keyRow, RowData... valueRows) {
        return contains(keyRow, Arrays.asList(valueRows));
    }
}
