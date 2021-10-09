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

import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.StringValue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.function.BiFunction;

import static org.apache.flink.configuration.ConfigurationUtils.canBePrefixMap;
import static org.apache.flink.configuration.ConfigurationUtils.containsPrefixMap;
import static org.apache.flink.configuration.ConfigurationUtils.convertToPropertiesPrefixed;
import static org.apache.flink.configuration.ConfigurationUtils.removePrefixMap;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Lightweight configuration object which stores key/value pairs. */
@Public
public class Configuration extends ExecutionConfig.GlobalJobParameters
        implements IOReadableWritable,
                java.io.Serializable,
                Cloneable,
                ReadableConfig,
                WritableConfig {

    private static final long serialVersionUID = 1L;

    private static final byte TYPE_STRING = 0;
    private static final byte TYPE_INT = 1;
    private static final byte TYPE_LONG = 2;
    private static final byte TYPE_BOOLEAN = 3;
    private static final byte TYPE_FLOAT = 4;
    private static final byte TYPE_DOUBLE = 5;
    private static final byte TYPE_BYTES = 6;

    /** The log object used for debugging. */
    private static final Logger LOG = LoggerFactory.getLogger(Configuration.class);

    /** Stores the concrete key/value pairs of this configuration object. */
    protected final HashMap<String, Object> confData;

    // --------------------------------------------------------------------------------------------

    /** Creates a new empty configuration. */
    public Configuration() {
        this.confData = new HashMap<>();
    }

    /**
     * Creates a new configuration with the copy of the given configuration.
     *
     * @param other The configuration to copy the entries from.
     */
    public Configuration(Configuration other) {
        this.confData = new HashMap<>(other.confData);
    }

    // --------------------------------------------------------------------------------------------

    /** Creates a new configuration that is initialized with the options of the given map. */
    public static Configuration fromMap(Map<String, String> map) {
        final Configuration configuration = new Configuration();
        map.forEach(configuration::setString);
        return configuration;
    }

    // --------------------------------------------------------------------------------------------

    /**
     * Returns the class associated with the given key as a string.
     *
     * @param <T> The type of the class to return.
     * @param key The key pointing to the associated value
     * @param defaultValue The optional default value returned if no entry exists
     * @param classLoader The class loader used to resolve the class.
     * @return The value associated with the given key, or the default value, if to entry for the
     *     key exists.
     */
    @SuppressWarnings("unchecked")
    public <T> Class<T> getClass(
            String key, Class<? extends T> defaultValue, ClassLoader classLoader)
            throws ClassNotFoundException {
        Optional<Object> o = getRawValue(key);
        if (!o.isPresent()) {
            return (Class<T>) defaultValue;
        }

        if (o.get().getClass() == String.class) {
            return (Class<T>) Class.forName((String) o.get(), true, classLoader);
        }

        throw new IllegalArgumentException(
                "Configuration cannot evaluate object of class "
                        + o.get().getClass()
                        + " as a class name");
    }

    /**
     * Adds the given key/value pair to the configuration object. The class can be retrieved by
     * invoking {@link #getClass(String, Class, ClassLoader)} if it is in the scope of the class
     * loader on the caller.
     *
     * @param key The key of the pair to be added
     * @param klazz The value of the pair to be added
     * @see #getClass(String, Class, ClassLoader)
     */
    public void setClass(String key, Class<?> klazz) {
        setValueInternal(key, klazz.getName());
    }

    /**
     * Returns the value associated with the given key as a string.
     *
     * @param key the key pointing to the associated value
     * @param defaultValue the default value which is returned in case there is no value associated
     *     with the given key
     * @return the (default) value associated with the given key
     * @deprecated use {@link #getString(ConfigOption, String)} or {@link
     *     #getOptional(ConfigOption)}
     */
    @Deprecated
    public String getString(String key, String defaultValue) {
        return getRawValue(key).map(ConfigurationUtils::convertToString).orElse(defaultValue);
    }

    /**
     * Returns the value associated with the given config option as a string.
     *
     * @param configOption The configuration option
     * @return the (default) value associated with the given config option
     */
    @PublicEvolving
    public String getString(ConfigOption<String> configOption) {
        return getOptional(configOption).orElseGet(configOption::defaultValue);
    }

    /**
     * Returns the value associated with the given config option as a string. If no value is mapped
     * under any key of the option, it returns the specified default instead of the option's default
     * value.
     *
     * @param configOption The configuration option
     * @return the (default) value associated with the given config option
     */
    @PublicEvolving
    public String getString(ConfigOption<String> configOption, String overrideDefault) {
        return getOptional(configOption).orElse(overrideDefault);
    }

    /**
     * Adds the given key/value pair to the configuration object.
     *
     * @param key the key of the key/value pair to be added
     * @param value the value of the key/value pair to be added
     */
    public void setString(String key, String value) {
        setValueInternal(key, value);
    }

    /**
     * Adds the given value to the configuration object. The main key of the config option will be
     * used to map the value.
     *
     * @param key the option specifying the key to be added
     * @param value the value of the key/value pair to be added
     */
    @PublicEvolving
    public void setString(ConfigOption<String> key, String value) {
        setValueInternal(key.key(), value);
    }

    /**
     * Returns the value associated with the given key as an integer.
     *
     * @param key the key pointing to the associated value
     * @param defaultValue the default value which is returned in case there is no value associated
     *     with the given key
     * @return the (default) value associated with the given key
     * @deprecated use {@link #getInteger(ConfigOption, int)} or {@link #getOptional(ConfigOption)}
     */
    @Deprecated
    public int getInteger(String key, int defaultValue) {
        return getRawValue(key).map(ConfigurationUtils::convertToInt).orElse(defaultValue);
    }

    /**
     * Returns the value associated with the given config option as an integer.
     *
     * @param configOption The configuration option
     * @return the (default) value associated with the given config option
     */
    @PublicEvolving
    public int getInteger(ConfigOption<Integer> configOption) {
        return getOptional(configOption).orElseGet(configOption::defaultValue);
    }

    /**
     * Returns the value associated with the given config option as an integer. If no value is
     * mapped under any key of the option, it returns the specified default instead of the option's
     * default value.
     *
     * @param configOption The configuration option
     * @param overrideDefault The value to return if no value was mapper for any key of the option
     * @return the configured value associated with the given config option, or the overrideDefault
     */
    @PublicEvolving
    public int getInteger(ConfigOption<Integer> configOption, int overrideDefault) {
        return getOptional(configOption).orElse(overrideDefault);
    }

    /**
     * Adds the given key/value pair to the configuration object.
     *
     * @param key the key of the key/value pair to be added
     * @param value the value of the key/value pair to be added
     */
    public void setInteger(String key, int value) {
        setValueInternal(key, value);
    }

    /**
     * Adds the given value to the configuration object. The main key of the config option will be
     * used to map the value.
     *
     * @param key the option specifying the key to be added
     * @param value the value of the key/value pair to be added
     */
    @PublicEvolving
    public void setInteger(ConfigOption<Integer> key, int value) {
        setValueInternal(key.key(), value);
    }

    /**
     * Returns the value associated with the given key as a long.
     *
     * @param key the key pointing to the associated value
     * @param defaultValue the default value which is returned in case there is no value associated
     *     with the given key
     * @return the (default) value associated with the given key
     * @deprecated use {@link #getLong(ConfigOption, long)} or {@link #getOptional(ConfigOption)}
     */
    @Deprecated
    public long getLong(String key, long defaultValue) {
        return getRawValue(key).map(ConfigurationUtils::convertToLong).orElse(defaultValue);
    }

    /**
     * Returns the value associated with the given config option as a long integer.
     *
     * @param configOption The configuration option
     * @return the (default) value associated with the given config option
     */
    @PublicEvolving
    public long getLong(ConfigOption<Long> configOption) {
        return getOptional(configOption).orElseGet(configOption::defaultValue);
    }

    /**
     * Returns the value associated with the given config option as a long integer. If no value is
     * mapped under any key of the option, it returns the specified default instead of the option's
     * default value.
     *
     * @param configOption The configuration option
     * @param overrideDefault The value to return if no value was mapper for any key of the option
     * @return the configured value associated with the given config option, or the overrideDefault
     */
    @PublicEvolving
    public long getLong(ConfigOption<Long> configOption, long overrideDefault) {
        return getOptional(configOption).orElse(overrideDefault);
    }

    /**
     * Adds the given key/value pair to the configuration object.
     *
     * @param key the key of the key/value pair to be added
     * @param value the value of the key/value pair to be added
     */
    public void setLong(String key, long value) {
        setValueInternal(key, value);
    }

    /**
     * Adds the given value to the configuration object. The main key of the config option will be
     * used to map the value.
     *
     * @param key the option specifying the key to be added
     * @param value the value of the key/value pair to be added
     */
    @PublicEvolving
    public void setLong(ConfigOption<Long> key, long value) {
        setValueInternal(key.key(), value);
    }

    /**
     * Returns the value associated with the given key as a boolean.
     *
     * @param key the key pointing to the associated value
     * @param defaultValue the default value which is returned in case there is no value associated
     *     with the given key
     * @return the (default) value associated with the given key
     * @deprecated use {@link #getBoolean(ConfigOption, boolean)} or {@link
     *     #getOptional(ConfigOption)}
     */
    @Deprecated
    public boolean getBoolean(String key, boolean defaultValue) {
        return getRawValue(key).map(ConfigurationUtils::convertToBoolean).orElse(defaultValue);
    }

    /**
     * Returns the value associated with the given config option as a boolean.
     *
     * @param configOption The configuration option
     * @return the (default) value associated with the given config option
     */
    @PublicEvolving
    public boolean getBoolean(ConfigOption<Boolean> configOption) {
        return getOptional(configOption).orElseGet(configOption::defaultValue);
    }

    /**
     * Returns the value associated with the given config option as a boolean. If no value is mapped
     * under any key of the option, it returns the specified default instead of the option's default
     * value.
     *
     * @param configOption The configuration option
     * @param overrideDefault The value to return if no value was mapper for any key of the option
     * @return the configured value associated with the given config option, or the overrideDefault
     */
    @PublicEvolving
    public boolean getBoolean(ConfigOption<Boolean> configOption, boolean overrideDefault) {
        return getOptional(configOption).orElse(overrideDefault);
    }

    /**
     * Adds the given key/value pair to the configuration object.
     *
     * @param key the key of the key/value pair to be added
     * @param value the value of the key/value pair to be added
     */
    public void setBoolean(String key, boolean value) {
        setValueInternal(key, value);
    }

    /**
     * Adds the given value to the configuration object. The main key of the config option will be
     * used to map the value.
     *
     * @param key the option specifying the key to be added
     * @param value the value of the key/value pair to be added
     */
    @PublicEvolving
    public void setBoolean(ConfigOption<Boolean> key, boolean value) {
        setValueInternal(key.key(), value);
    }

    /**
     * Returns the value associated with the given key as a float.
     *
     * @param key the key pointing to the associated value
     * @param defaultValue the default value which is returned in case there is no value associated
     *     with the given key
     * @return the (default) value associated with the given key
     * @deprecated use {@link #getFloat(ConfigOption, float)} or {@link #getOptional(ConfigOption)}
     */
    @Deprecated
    public float getFloat(String key, float defaultValue) {
        return getRawValue(key).map(ConfigurationUtils::convertToFloat).orElse(defaultValue);
    }

    /**
     * Returns the value associated with the given config option as a float.
     *
     * @param configOption The configuration option
     * @return the (default) value associated with the given config option
     */
    @PublicEvolving
    public float getFloat(ConfigOption<Float> configOption) {
        return getOptional(configOption).orElseGet(configOption::defaultValue);
    }

    /**
     * Returns the value associated with the given config option as a float. If no value is mapped
     * under any key of the option, it returns the specified default instead of the option's default
     * value.
     *
     * @param configOption The configuration option
     * @param overrideDefault The value to return if no value was mapper for any key of the option
     * @return the configured value associated with the given config option, or the overrideDefault
     */
    @PublicEvolving
    public float getFloat(ConfigOption<Float> configOption, float overrideDefault) {
        return getOptional(configOption).orElse(overrideDefault);
    }

    /**
     * Adds the given key/value pair to the configuration object.
     *
     * @param key the key of the key/value pair to be added
     * @param value the value of the key/value pair to be added
     */
    public void setFloat(String key, float value) {
        setValueInternal(key, value);
    }

    /**
     * Adds the given value to the configuration object. The main key of the config option will be
     * used to map the value.
     *
     * @param key the option specifying the key to be added
     * @param value the value of the key/value pair to be added
     */
    @PublicEvolving
    public void setFloat(ConfigOption<Float> key, float value) {
        setValueInternal(key.key(), value);
    }

    /**
     * Returns the value associated with the given key as a double.
     *
     * @param key the key pointing to the associated value
     * @param defaultValue the default value which is returned in case there is no value associated
     *     with the given key
     * @return the (default) value associated with the given key
     * @deprecated use {@link #getDouble(ConfigOption, double)} or {@link
     *     #getOptional(ConfigOption)}
     */
    @Deprecated
    public double getDouble(String key, double defaultValue) {
        return getRawValue(key).map(ConfigurationUtils::convertToDouble).orElse(defaultValue);
    }

    /**
     * Returns the value associated with the given config option as a {@code double}.
     *
     * @param configOption The configuration option
     * @return the (default) value associated with the given config option
     */
    @PublicEvolving
    public double getDouble(ConfigOption<Double> configOption) {
        return getOptional(configOption).orElseGet(configOption::defaultValue);
    }

    /**
     * Returns the value associated with the given config option as a {@code double}. If no value is
     * mapped under any key of the option, it returns the specified default instead of the option's
     * default value.
     *
     * @param configOption The configuration option
     * @param overrideDefault The value to return if no value was mapper for any key of the option
     * @return the configured value associated with the given config option, or the overrideDefault
     */
    @PublicEvolving
    public double getDouble(ConfigOption<Double> configOption, double overrideDefault) {
        return getOptional(configOption).orElse(overrideDefault);
    }

    /**
     * Adds the given key/value pair to the configuration object.
     *
     * @param key the key of the key/value pair to be added
     * @param value the value of the key/value pair to be added
     */
    public void setDouble(String key, double value) {
        setValueInternal(key, value);
    }

    /**
     * Adds the given value to the configuration object. The main key of the config option will be
     * used to map the value.
     *
     * @param key the option specifying the key to be added
     * @param value the value of the key/value pair to be added
     */
    @PublicEvolving
    public void setDouble(ConfigOption<Double> key, double value) {
        setValueInternal(key.key(), value);
    }

    /**
     * Returns the value associated with the given key as a byte array.
     *
     * @param key The key pointing to the associated value.
     * @param defaultValue The default value which is returned in case there is no value associated
     *     with the given key.
     * @return the (default) value associated with the given key.
     */
    public byte[] getBytes(String key, byte[] defaultValue) {
        return getRawValue(key)
                .map(
                        o -> {
                            if (o.getClass().equals(byte[].class)) {
                                return (byte[]) o;
                            } else {
                                throw new IllegalArgumentException(
                                        String.format(
                                                "Configuration cannot evaluate value %s as a byte[] value",
                                                o));
                            }
                        })
                .orElse(defaultValue);
    }

    /**
     * Adds the given byte array to the configuration object. If key is <code>null</code> then
     * nothing is added.
     *
     * @param key The key under which the bytes are added.
     * @param bytes The bytes to be added.
     */
    public void setBytes(String key, byte[] bytes) {
        setValueInternal(key, bytes);
    }

    /**
     * Returns the value associated with the given config option as a string.
     *
     * @param configOption The configuration option
     * @return the (default) value associated with the given config option
     */
    @PublicEvolving
    public String getValue(ConfigOption<?> configOption) {
        return Optional.ofNullable(
                        getRawValueFromOption(configOption).orElseGet(configOption::defaultValue))
                .map(String::valueOf)
                .orElse(null);
    }

    /**
     * Returns the value associated with the given config option as an enum.
     *
     * @param enumClass The return enum class
     * @param configOption The configuration option
     * @throws IllegalArgumentException If the string associated with the given config option cannot
     *     be parsed as a value of the provided enum class.
     */
    @PublicEvolving
    public <T extends Enum<T>> T getEnum(
            final Class<T> enumClass, final ConfigOption<String> configOption) {
        checkNotNull(enumClass, "enumClass must not be null");
        checkNotNull(configOption, "configOption must not be null");

        Object rawValue = getRawValueFromOption(configOption).orElseGet(configOption::defaultValue);
        try {
            return ConfigurationUtils.convertToEnum(rawValue, enumClass);
        } catch (IllegalArgumentException ex) {
            final String errorMessage =
                    String.format(
                            "Value for config option %s must be one of %s (was %s)",
                            configOption.key(),
                            Arrays.toString(enumClass.getEnumConstants()),
                            rawValue);
            throw new IllegalArgumentException(errorMessage);
        }
    }

    // --------------------------------------------------------------------------------------------

    /**
     * Returns the keys of all key/value pairs stored inside this configuration object.
     *
     * @return the keys of all key/value pairs stored inside this configuration object
     */
    public Set<String> keySet() {
        synchronized (this.confData) {
            return new HashSet<>(this.confData.keySet());
        }
    }

    /** Adds all entries in this {@code Configuration} to the given {@link Properties}. */
    public void addAllToProperties(Properties props) {
        synchronized (this.confData) {
            for (Map.Entry<String, Object> entry : this.confData.entrySet()) {
                props.put(entry.getKey(), entry.getValue());
            }
        }
    }

    public void addAll(Configuration other) {
        synchronized (this.confData) {
            synchronized (other.confData) {
                this.confData.putAll(other.confData);
            }
        }
    }

    /**
     * Adds all entries from the given configuration into this configuration. The keys are prepended
     * with the given prefix.
     *
     * @param other The configuration whose entries are added to this configuration.
     * @param prefix The prefix to prepend.
     */
    public void addAll(Configuration other, String prefix) {
        final StringBuilder bld = new StringBuilder();
        bld.append(prefix);
        final int pl = bld.length();

        synchronized (this.confData) {
            synchronized (other.confData) {
                for (Map.Entry<String, Object> entry : other.confData.entrySet()) {
                    bld.setLength(pl);
                    bld.append(entry.getKey());
                    this.confData.put(bld.toString(), entry.getValue());
                }
            }
        }
    }

    @Override
    public Configuration clone() {
        Configuration config = new Configuration();
        config.addAll(this);

        return config;
    }

    /**
     * Checks whether there is an entry with the specified key.
     *
     * @param key key of entry
     * @return true if the key is stored, false otherwise
     */
    public boolean containsKey(String key) {
        synchronized (this.confData) {
            return this.confData.containsKey(key);
        }
    }

    /**
     * Checks whether there is an entry for the given config option.
     *
     * @param configOption The configuration option
     * @return <tt>true</tt> if a valid (current or deprecated) key of the config option is stored,
     *     <tt>false</tt> otherwise
     */
    @PublicEvolving
    public boolean contains(ConfigOption<?> configOption) {
        synchronized (this.confData) {
            final BiFunction<String, Boolean, Optional<Boolean>> applier =
                    (key, canBePrefixMap) -> {
                        if (canBePrefixMap && containsPrefixMap(this.confData, key)
                                || this.confData.containsKey(key)) {
                            return Optional.of(true);
                        }
                        return Optional.empty();
                    };
            return applyWithOption(configOption, applier).orElse(false);
        }
    }

    private <T> Optional<T> applyWithOption(
            ConfigOption<?> option, BiFunction<String, Boolean, Optional<T>> applier) {
        final boolean canBePrefixMap = canBePrefixMap(option);
        final Optional<T> valueFromExactKey = applier.apply(option.key(), canBePrefixMap);
        if (valueFromExactKey.isPresent()) {
            return valueFromExactKey;
        } else if (option.hasFallbackKeys()) {
            // try the fallback keys
            for (FallbackKey fallbackKey : option.fallbackKeys()) {
                final Optional<T> valueFromFallbackKey =
                        applier.apply(fallbackKey.getKey(), canBePrefixMap);
                if (valueFromFallbackKey.isPresent()) {
                    loggingFallback(fallbackKey, option);
                    return valueFromFallbackKey;
                }
            }
        }
        return Optional.empty();
    }

    @Override
    public <T> T get(ConfigOption<T> option) {
        return getOptional(option).orElseGet(option::defaultValue);
    }

    @Override
    public <T> Optional<T> getOptional(ConfigOption<T> option) {
        Optional<Object> rawValue = getRawValueFromOption(option);
        Class<?> clazz = option.getClazz();

        try {
            if (option.isList()) {
                return rawValue.map(v -> ConfigurationUtils.convertToList(v, clazz));
            } else {
                return rawValue.map(v -> ConfigurationUtils.convertValue(v, clazz));
            }
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    String.format(
                            "Could not parse value '%s' for key '%s'.",
                            rawValue.map(Object::toString).orElse(""), option.key()),
                    e);
        }
    }

    @Override
    public <T> Configuration set(ConfigOption<T> option, T value) {
        final boolean canBePrefixMap = canBePrefixMap(option);
        setValueInternal(option.key(), value, canBePrefixMap);
        return this;
    }

    // --------------------------------------------------------------------------------------------

    @Override
    public Map<String, String> toMap() {
        synchronized (this.confData) {
            Map<String, String> ret = new HashMap<>(this.confData.size());
            for (Map.Entry<String, Object> entry : confData.entrySet()) {
                ret.put(entry.getKey(), ConfigurationUtils.convertToString(entry.getValue()));
            }
            return ret;
        }
    }

    /**
     * Removes given config option from the configuration.
     *
     * @param configOption config option to remove
     * @param <T> Type of the config option
     * @return true is config has been removed, false otherwise
     */
    public <T> boolean removeConfig(ConfigOption<T> configOption) {
        synchronized (this.confData) {
            final BiFunction<String, Boolean, Optional<Boolean>> applier =
                    (key, canBePrefixMap) -> {
                        if (canBePrefixMap && removePrefixMap(this.confData, key)
                                || this.confData.remove(key) != null) {
                            return Optional.of(true);
                        }
                        return Optional.empty();
                    };
            return applyWithOption(configOption, applier).orElse(false);
        }
    }

    // --------------------------------------------------------------------------------------------

    <T> void setValueInternal(String key, T value, boolean canBePrefixMap) {
        if (key == null) {
            throw new NullPointerException("Key must not be null.");
        }
        if (value == null) {
            throw new NullPointerException("Value must not be null.");
        }

        synchronized (this.confData) {
            if (canBePrefixMap) {
                removePrefixMap(this.confData, key);
            }
            this.confData.put(key, value);
        }
    }

    private <T> void setValueInternal(String key, T value) {
        setValueInternal(key, value, false);
    }

    private Optional<Object> getRawValue(String key) {
        return getRawValue(key, false);
    }

    private Optional<Object> getRawValue(String key, boolean canBePrefixMap) {
        if (key == null) {
            throw new NullPointerException("Key must not be null.");
        }

        synchronized (this.confData) {
            final Object valueFromExactKey = this.confData.get(key);
            if (!canBePrefixMap || valueFromExactKey != null) {
                return Optional.ofNullable(valueFromExactKey);
            }
            final Map<String, String> valueFromPrefixMap =
                    convertToPropertiesPrefixed(confData, key);
            if (valueFromPrefixMap.isEmpty()) {
                return Optional.empty();
            }
            return Optional.of(valueFromPrefixMap);
        }
    }

    private Optional<Object> getRawValueFromOption(ConfigOption<?> configOption) {
        return applyWithOption(configOption, this::getRawValue);
    }

    private void loggingFallback(FallbackKey fallbackKey, ConfigOption<?> configOption) {
        if (fallbackKey.isDeprecated()) {
            LOG.warn(
                    "Config uses deprecated configuration key '{}' instead of proper key '{}'",
                    fallbackKey.getKey(),
                    configOption.key());
        } else {
            LOG.info(
                    "Config uses fallback configuration key '{}' instead of key '{}'",
                    fallbackKey.getKey(),
                    configOption.key());
        }
    }

    // --------------------------------------------------------------------------------------------
    //  Serialization
    // --------------------------------------------------------------------------------------------

    @Override
    public void read(DataInputView in) throws IOException {
        synchronized (this.confData) {
            final int numberOfProperties = in.readInt();

            for (int i = 0; i < numberOfProperties; i++) {
                String key = StringValue.readString(in);
                Object value;

                byte type = in.readByte();
                switch (type) {
                    case TYPE_STRING:
                        value = StringValue.readString(in);
                        break;
                    case TYPE_INT:
                        value = in.readInt();
                        break;
                    case TYPE_LONG:
                        value = in.readLong();
                        break;
                    case TYPE_FLOAT:
                        value = in.readFloat();
                        break;
                    case TYPE_DOUBLE:
                        value = in.readDouble();
                        break;
                    case TYPE_BOOLEAN:
                        value = in.readBoolean();
                        break;
                    case TYPE_BYTES:
                        byte[] bytes = new byte[in.readInt()];
                        in.readFully(bytes);
                        value = bytes;
                        break;
                    default:
                        throw new IOException(
                                String.format(
                                        "Unrecognized type: %s. This method is deprecated and"
                                                + " might not work for all supported types.",
                                        type));
                }

                this.confData.put(key, value);
            }
        }
    }

    @Override
    public void write(final DataOutputView out) throws IOException {
        synchronized (this.confData) {
            out.writeInt(this.confData.size());

            for (Map.Entry<String, Object> entry : this.confData.entrySet()) {
                String key = entry.getKey();
                Object val = entry.getValue();

                StringValue.writeString(key, out);
                Class<?> clazz = val.getClass();

                if (clazz == String.class) {
                    out.write(TYPE_STRING);
                    StringValue.writeString((String) val, out);
                } else if (clazz == Integer.class) {
                    out.write(TYPE_INT);
                    out.writeInt((Integer) val);
                } else if (clazz == Long.class) {
                    out.write(TYPE_LONG);
                    out.writeLong((Long) val);
                } else if (clazz == Float.class) {
                    out.write(TYPE_FLOAT);
                    out.writeFloat((Float) val);
                } else if (clazz == Double.class) {
                    out.write(TYPE_DOUBLE);
                    out.writeDouble((Double) val);
                } else if (clazz == byte[].class) {
                    out.write(TYPE_BYTES);
                    byte[] bytes = (byte[]) val;
                    out.writeInt(bytes.length);
                    out.write(bytes);
                } else if (clazz == Boolean.class) {
                    out.write(TYPE_BOOLEAN);
                    out.writeBoolean((Boolean) val);
                } else {
                    throw new IllegalArgumentException(
                            "Unrecognized type. This method is deprecated and might not work"
                                    + " for all supported types.");
                }
            }
        }
    }

    // --------------------------------------------------------------------------------------------

    @Override
    public int hashCode() {
        int hash = 0;
        for (String s : this.confData.keySet()) {
            hash ^= s.hashCode();
        }
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (obj instanceof Configuration) {
            Map<String, Object> otherConf = ((Configuration) obj).confData;

            for (Map.Entry<String, Object> e : this.confData.entrySet()) {
                Object thisVal = e.getValue();
                Object otherVal = otherConf.get(e.getKey());

                if (!thisVal.getClass().equals(byte[].class)) {
                    if (!thisVal.equals(otherVal)) {
                        return false;
                    }
                } else if (otherVal.getClass().equals(byte[].class)) {
                    if (!Arrays.equals((byte[]) thisVal, (byte[]) otherVal)) {
                        return false;
                    }
                } else {
                    return false;
                }
            }

            return true;
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return this.confData.toString();
    }
}
