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

package org.apache.flink.table.factories;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.module.Module;
import org.apache.flink.table.module.ModuleException;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A factory to create configured module instances based on string-based properties. See also {@link
 * Factory} for more information.
 *
 * <p>Note that this interface supports the {@link TableFactory} stack for compatibility purposes.
 * This is deprecated, however, and new implementations should implement the {@link Factory} stack
 * instead.
 */
@PublicEvolving
public interface ModuleFactory extends TableFactory, Factory {

    /**
     * Creates and configures a {@link Module} using the given properties.
     *
     * @param properties normalized properties describing a module.
     * @return the configured module.
     * @deprecated Use {@link #createModule(Context)} instead and implement {@link Factory} instead
     *     of {@link TableFactory}.
     */
    @Deprecated
    default Module createModule(Map<String, String> properties) {
        throw new ModuleException("Module factories must implement createModule().");
    }

    /** Creates and configures a {@link Module}. */
    default Module createModule(Context context) {
        throw new ModuleException("Module factories must implement createModule(Context).");
    }

    /** Context provided when a module is created. */
    interface Context {
        /**
         * Returns the options with which the module is created.
         *
         * <p>An implementation should perform validation of these options.
         */
        Map<String, String> getOptions();

        /** Gives read-only access to the configuration of the current session. */
        ReadableConfig getConfiguration();

        /**
         * Returns the class loader of the current session.
         *
         * <p>The class loader is in particular useful for discovering further (nested) factories.
         */
        ClassLoader getClassLoader();
    }

    default String factoryIdentifier() {
        if (requiredContext() == null || supportedProperties() == null) {
            throw new ModuleException("Module factories must implement factoryIdentifier()");
        }

        return null;
    }

    default Set<ConfigOption<?>> requiredOptions() {
        if (requiredContext() == null || supportedProperties() == null) {
            throw new ModuleException("Module factories must implement requiredOptions()");
        }

        return null;
    }

    default Set<ConfigOption<?>> optionalOptions() {
        if (requiredContext() == null || supportedProperties() == null) {
            throw new ModuleException("Module factories must implement optionalOptions()");
        }

        return null;
    }

    // --------------------------------------------------------------------------------------------
    // Default implementations for legacy {@link TableFactory} stack.
    // --------------------------------------------------------------------------------------------

    /** @deprecated Implement the {@link Factory} based stack instead. */
    @Deprecated
    default Map<String, String> requiredContext() {
        // Default implementation for modules implementing the new {@link Factory} stack instead.
        return null;
    }

    /** @deprecated Implement the {@link Factory} based stack instead. */
    @Deprecated
    default List<String> supportedProperties() {
        // Default implementation for modules implementing the new {@link Factory} stack instead.
        return null;
    }
}
