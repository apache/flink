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

package org.apache.flink.table.api;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.FunctionLanguage;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.functions.UserDefinedFunctionHelper;
import org.apache.flink.table.resource.ResourceUri;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Describes a {@link CatalogFunction}.
 *
 * <p>A {@link FunctionDescriptor} is a template for creating a {@link CatalogFunction} instance. It
 * closely resembles the "CREATE FUNCTION" SQL DDL statement.
 *
 * <p>This can be used to register a table in the Table API, see {@link
 * TableEnvironment#createFunction(String, FunctionDescriptor)}.
 */
@PublicEvolving
public class FunctionDescriptor {

    private final String className;
    private final FunctionLanguage language;
    private final List<ResourceUri> resourceUris;
    private final Map<String, String> options;

    private FunctionDescriptor(
            String className,
            FunctionLanguage language,
            List<ResourceUri> resourceUris,
            Map<String, String> options) {
        this.className = className;
        this.language = language;
        this.resourceUris = resourceUris;
        this.options = options;
    }

    /** Creates a {@link Builder} for a function descriptor with the given class name. */
    public static Builder forClassName(String className) {
        return new Builder(className);
    }

    /** Creates a {@link Builder} for a function descriptor for the given function class. */
    public static Builder forFunctionClass(Class<? extends UserDefinedFunction> functionClass) {
        try {
            UserDefinedFunctionHelper.validateClass(functionClass);
        } catch (Throwable t) {
            throw new ValidationException(
                    String.format(
                            "Can not create a function '%s' due to implementation errors.",
                            functionClass.getName()),
                    t);
        }
        return new Builder(functionClass.getName()).language(FunctionLanguage.JAVA);
    }

    public String getClassName() {
        return className;
    }

    public FunctionLanguage getLanguage() {
        return language;
    }

    public List<ResourceUri> getResourceUris() {
        return resourceUris;
    }

    public Map<String, String> getOptions() {
        return options;
    }

    /** Builder for {@link FunctionDescriptor}. */
    @PublicEvolving
    public static final class Builder {
        private final String className;
        private FunctionLanguage language = FunctionLanguage.JAVA;
        private final List<ResourceUri> resourceUris = new ArrayList<>();
        private final Map<String, String> options = new HashMap<>();

        private Builder(String className) {
            this.className = className;
        }

        /**
         * Sets the language of the function. Equivalent to the {@code LANGUAGE} clause in the
         * "CREATE FUNCTION" SQL DDL statement.
         */
        public Builder language(FunctionLanguage language) {
            Preconditions.checkNotNull(language, "Function language must not be null.");
            this.language = language;
            return this;
        }

        /**
         * Adds a list of resource URIs to the function descriptor. Equivalent to the {@code USING}
         * clause in the "CREATE FUNCTION" SQL DDL statement.
         */
        public Builder resourceUris(List<ResourceUri> uri) {
            Preconditions.checkNotNull(uri, "Resource URIs must not be null.");
            this.resourceUris.addAll(uri);
            return this;
        }

        /**
         * Adds a single resource URI to the function descriptor. Equivalent to the {@code USING}
         * clause in the "CREATE FUNCTION" SQL DDL statement.
         */
        public Builder resourceUri(ResourceUri uri) {
            Preconditions.checkNotNull(uri, "Resource URI must not be null.");
            this.resourceUris.add(uri);
            return this;
        }

        /**
         * Adds an option to the function descriptor. Equivalent to the {@code WITH} clause in the
         * "CREATE FUNCTION" SQL DDL statement.
         */
        public Builder option(String key, String value) {
            Preconditions.checkNotNull(key, "Option key must not be null.");
            Preconditions.checkNotNull(value, "Option value must not be null.");
            this.options.put(key, value);
            return this;
        }

        /**
         * Adds multiple options to the function descriptor. Equivalent to the {@code WITH} clause
         * in the "CREATE FUNCTION" SQL DDL statement.
         */
        public Builder options(Map<String, String> options) {
            Preconditions.checkNotNull(options, "Options must not be null.");
            this.options.putAll(options);
            return this;
        }

        public FunctionDescriptor build() {
            return new FunctionDescriptor(
                    className,
                    language,
                    Collections.unmodifiableList(resourceUris),
                    Collections.unmodifiableMap(options));
        }
    }
}
