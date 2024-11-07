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

package org.apache.flink.configuration;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import static org.apache.flink.configuration.FallbackKey.createDeprecatedKey;

/**
 * A configuration that manages a subset of keys with a common prefix from a given configuration.
 */
public final class DelegatingConfiguration extends Configuration {

    private static final long serialVersionUID = 1L;

    private final Configuration backingConfig; // the configuration actually storing the data

    @Nonnull private String prefix; // the prefix key by which keys for this config are marked

    // --------------------------------------------------------------------------------------------

    /** Default constructor for serialization. Creates an empty delegating configuration. */
    public DelegatingConfiguration() {
        this(new Configuration(), "");
    }

    /**
     * Creates a new delegating configuration which stores its key/value pairs in the given
     * configuration using the specifies key prefix.
     *
     * @param backingConfig The configuration holding the actual config data.
     * @param prefix The prefix prepended to all config keys.
     */
    public DelegatingConfiguration(Configuration backingConfig, String prefix) {
        this.backingConfig = Preconditions.checkNotNull(backingConfig);
        this.prefix = Preconditions.checkNotNull(prefix, "The 'prefix' attribute mustn't be null.");
    }

    // --------------------------------------------------------------------------------------------

    @Override
    public String getString(String key, String defaultValue) {
        return this.backingConfig.getString(this.prefix + key, defaultValue);
    }

    @Override
    public void setString(String key, String value) {
        this.backingConfig.setString(this.prefix + key, value);
    }

    @Override
    public byte[] getBytes(final String key, final byte[] defaultValue) {
        return this.backingConfig.getBytes(this.prefix + key, defaultValue);
    }

    @Override
    public void setBytes(final String key, final byte[] bytes) {
        this.backingConfig.setBytes(this.prefix + key, bytes);
    }

    @Override
    public String getValue(ConfigOption<?> configOption) {
        return this.backingConfig.getValue(prefixOption(configOption, prefix));
    }

    @Override
    public <T extends Enum<T>> T getEnum(
            final Class<T> enumClass, final ConfigOption<String> configOption) {
        return this.backingConfig.getEnum(enumClass, prefixOption(configOption, prefix));
    }

    @Override
    public void addAllToProperties(Properties props) {
        // only add keys with our prefix
        synchronized (backingConfig.confData) {
            for (Map.Entry<String, Object> entry : backingConfig.confData.entrySet()) {
                if (entry.getKey().startsWith(prefix)) {
                    String keyWithoutPrefix =
                            entry.getKey().substring(prefix.length(), entry.getKey().length());

                    props.put(keyWithoutPrefix, entry.getValue());
                } else {
                    // don't add stuff that doesn't have our prefix
                }
            }
        }
    }

    @Override
    public void addAll(Configuration other) {
        this.addAll(other, "");
    }

    @Override
    public void addAll(Configuration other, String prefix) {
        this.backingConfig.addAll(other, this.prefix + prefix);
    }

    @Override
    public String toString() {
        return backingConfig.toString();
    }

    @Override
    public Set<String> keySet() {
        if (this.prefix.isEmpty()) {
            return this.backingConfig.keySet();
        }

        final HashSet<String> set = new HashSet<>();
        int prefixLen = this.prefix.length();

        for (String key : this.backingConfig.keySet()) {
            if (key.startsWith(prefix)) {
                set.add(key.substring(prefixLen));
            }
        }

        return set;
    }

    @Override
    public Configuration clone() {
        return new DelegatingConfiguration(backingConfig.clone(), prefix);
    }

    @Override
    public Map<String, String> toMap() {
        Map<String, String> map = backingConfig.toMap();
        Map<String, String> prefixed = new HashMap<>();
        for (Map.Entry<String, String> entry : map.entrySet()) {
            if (entry.getKey().startsWith(prefix)) {
                String keyWithoutPrefix = entry.getKey().substring(prefix.length());
                prefixed.put(keyWithoutPrefix, entry.getValue());
            }
        }
        return prefixed;
    }

    @Override
    public Map<String, String> toFileWritableMap() {
        Map<String, String> map = backingConfig.toFileWritableMap();
        Map<String, String> prefixed = new HashMap<>();
        for (Map.Entry<String, String> entry : map.entrySet()) {
            if (entry.getKey().startsWith(prefix)) {
                String keyWithoutPrefix = entry.getKey().substring(prefix.length());
                prefixed.put(keyWithoutPrefix, YamlParserUtils.toYAMLString(entry.getValue()));
            }
        }
        return prefixed;
    }

    @Override
    public <T> boolean removeConfig(ConfigOption<T> configOption) {
        return backingConfig.removeConfig(prefixOption(configOption, prefix));
    }

    @Override
    public boolean removeKey(String key) {
        return backingConfig.removeKey(prefix + key);
    }

    @Override
    public boolean containsKey(String key) {
        return backingConfig.containsKey(prefix + key);
    }

    @Override
    public boolean contains(ConfigOption<?> configOption) {
        return backingConfig.contains(prefixOption(configOption, prefix));
    }

    @Override
    public <T> T get(ConfigOption<T> option) {
        return backingConfig.get(prefixOption(option, prefix));
    }

    @Override
    public <T> T get(ConfigOption<T> configOption, T overrideDefault) {
        return backingConfig.get(prefixOption(configOption, prefix), overrideDefault);
    }

    @Override
    public <T> Optional<T> getOptional(ConfigOption<T> option) {
        return backingConfig.getOptional(prefixOption(option, prefix));
    }

    @Override
    public <T> Configuration set(ConfigOption<T> option, T value) {
        backingConfig.set(prefixOption(option, prefix), value);
        return this;
    }

    // --------------------------------------------------------------------------------------------

    @Override
    public void read(DataInputView in) throws IOException {
        this.prefix = Preconditions.checkNotNull(in.readUTF());
        this.backingConfig.read(in);
    }

    @Override
    public void write(DataOutputView out) throws IOException {
        out.writeUTF(this.prefix);
        this.backingConfig.write(out);
    }

    // --------------------------------------------------------------------------------------------

    @Override
    public int hashCode() {
        return this.prefix.hashCode() ^ this.backingConfig.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof DelegatingConfiguration) {
            DelegatingConfiguration other = (DelegatingConfiguration) obj;
            return this.prefix.equals(other.prefix)
                    && this.backingConfig.equals(other.backingConfig);
        } else {
            return false;
        }
    }

    // --------------------------------------------------------------------------------------------

    private static <T> ConfigOption<T> prefixOption(ConfigOption<T> option, String prefix) {
        String key = prefix + option.key();

        List<FallbackKey> deprecatedKeys;
        if (option.hasFallbackKeys()) {
            deprecatedKeys = new ArrayList<>();
            for (FallbackKey dk : option.fallbackKeys()) {
                deprecatedKeys.add(createDeprecatedKey(prefix + dk.getKey()));
            }
        } else {
            deprecatedKeys = Collections.emptyList();
        }

        FallbackKey[] deprecated = deprecatedKeys.toArray(new FallbackKey[0]);
        return new ConfigOption<T>(
                key,
                option.getClazz(),
                option.description(),
                option.defaultValue(),
                option.isList(),
                deprecated);
    }
}
