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
import org.apache.flink.table.resource.ResourceUri;
import org.apache.flink.util.StringUtils;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** A catalog function implementation. */
@Internal
public class CatalogFunctionImpl implements CatalogFunction {
    private final String className; // Fully qualified class name of the function
    private final FunctionLanguage functionLanguage;
    private final List<ResourceUri> resourceUris;
    private final Map<String, String> options;

    public CatalogFunctionImpl(String className) {
        this(className, FunctionLanguage.JAVA, Collections.emptyList());
    }

    public CatalogFunctionImpl(String className, FunctionLanguage functionLanguage) {
        this(className, functionLanguage, Collections.emptyList());
    }

    public CatalogFunctionImpl(
            String className, FunctionLanguage functionLanguage, List<ResourceUri> resourceUris) {
        this(className, functionLanguage, resourceUris, Collections.emptyMap());
    }

    public CatalogFunctionImpl(
            String className,
            FunctionLanguage functionLanguage,
            List<ResourceUri> resourceUris,
            Map<String, String> options) {
        checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(className),
                "className cannot be null or empty");
        this.className = className;
        this.functionLanguage = checkNotNull(functionLanguage, "functionLanguage cannot be null");
        this.resourceUris = resourceUris;
        this.options = checkNotNull(options, "options cannot be null");
    }

    @Override
    public String getClassName() {
        return this.className;
    }

    @Override
    public CatalogFunction copy() {
        return new CatalogFunctionImpl(
                getClassName(),
                functionLanguage,
                Collections.unmodifiableList(resourceUris),
                Collections.unmodifiableMap(options));
    }

    @Override
    public Optional<String> getDescription() {
        return Optional.of("This is a user-defined function");
    }

    @Override
    public Optional<String> getDetailedDescription() {
        return Optional.of("This is a user-defined function");
    }

    @Override
    public FunctionLanguage getFunctionLanguage() {
        return functionLanguage;
    }

    @Override
    public List<ResourceUri> getFunctionResources() {
        return resourceUris;
    }

    @Override
    public Map<String, String> getOptions() {
        return options;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        CatalogFunctionImpl that = (CatalogFunctionImpl) o;
        return Objects.equals(className, that.className)
                && functionLanguage == that.functionLanguage
                && Objects.equals(resourceUris, that.resourceUris)
                && Objects.equals(options, that.options);
    }

    @Override
    public int hashCode() {
        int result = Objects.hashCode(className);
        result = 31 * result + Objects.hashCode(functionLanguage);
        result = 31 * result + Objects.hashCode(resourceUris);
        result = 31 * result + Objects.hashCode(options);
        return result;
    }

    @Override
    public String toString() {
        return "CatalogFunctionImpl{"
                + "className='"
                + getClassName()
                + "', "
                + "functionLanguage='"
                + getFunctionLanguage()
                + "', "
                + "functionResource='"
                + getFunctionResources()
                + "', "
                + "options='"
                + getOptions()
                + "'}";
    }
}
