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

package org.apache.flink.runtime.security;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * A dynamic JAAS configuration.
 *
 * <p>Makes it possible to define Application Configuration Entries (ACEs) at runtime, building upon
 * an (optional) underlying configuration. Entries from the underlying configuration take precedence
 * over dynamic entries.
 */
public class DynamicConfiguration extends Configuration {

    protected static final Logger LOG = LoggerFactory.getLogger(DynamicConfiguration.class);

    private final Configuration delegate;

    private final Map<String, AppConfigurationEntry[]> dynamicEntries = new HashMap<>();

    /**
     * Create a dynamic configuration.
     *
     * @param delegate an underlying configuration to delegate to, or null.
     */
    public DynamicConfiguration(@Nullable Configuration delegate) {
        this.delegate = delegate;
    }

    /** Add entries for the given application name. */
    public void addAppConfigurationEntry(String name, AppConfigurationEntry... entry) {
        final AppConfigurationEntry[] existing = dynamicEntries.get(name);
        final AppConfigurationEntry[] updated;
        if (existing == null) {
            updated = Arrays.copyOf(entry, entry.length);
        } else {
            updated = merge(existing, entry);
        }
        dynamicEntries.put(name, updated);
    }

    /**
     * Retrieve the AppConfigurationEntries for the specified <i>name</i> from this Configuration.
     *
     * @param name the name used to index the Configuration.
     * @return an array of AppConfigurationEntries for the specified <i>name</i> from this
     *     Configuration, or null if there are no entries for the specified <i>name</i>
     */
    @Override
    public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
        AppConfigurationEntry[] entry = null;
        if (delegate != null) {
            entry = delegate.getAppConfigurationEntry(name);
        }
        final AppConfigurationEntry[] existing = dynamicEntries.get(name);
        if (existing != null) {
            if (entry != null) {
                entry = merge(entry, existing);
            } else {
                entry = Arrays.copyOf(existing, existing.length);
            }
        }
        return entry;
    }

    private static AppConfigurationEntry[] merge(
            AppConfigurationEntry[] a, AppConfigurationEntry[] b) {
        AppConfigurationEntry[] merged = Arrays.copyOf(a, a.length + b.length);
        System.arraycopy(b, 0, merged, a.length, b.length);
        return merged;
    }

    @Override
    public void refresh() {
        if (delegate != null) {
            delegate.refresh();
        }
    }
}
