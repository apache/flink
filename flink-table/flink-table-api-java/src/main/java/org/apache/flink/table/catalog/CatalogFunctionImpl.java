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

import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.util.StringUtils;

import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** A catalog function implementation. */
public class CatalogFunctionImpl implements CatalogFunction {
    private final String className; // Fully qualified class name of the function
    private final FunctionLanguage functionLanguage;

    public CatalogFunctionImpl(String className) {
        this(className, FunctionLanguage.JAVA);
    }

    public CatalogFunctionImpl(String className, FunctionLanguage functionLanguage) {
        checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(className),
                "className cannot be null or empty");
        this.className = className;
        this.functionLanguage = checkNotNull(functionLanguage, "functionLanguage cannot be null");
    }

    @Override
    public String getClassName() {
        return this.className;
    }

    @Override
    public CatalogFunction copy() {
        return new CatalogFunctionImpl(getClassName(), functionLanguage);
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
    public boolean isGeneric() {
        if (functionLanguage == FunctionLanguage.PYTHON) {
            return true;
        }
        try {
            ClassLoader cl = Thread.currentThread().getContextClassLoader();
            Class c = Class.forName(className, true, cl);
            if (UserDefinedFunction.class.isAssignableFrom(c)) {
                return true;
            }
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(String.format("Can't resolve udf class %s", className), e);
        }
        return false;
    }

    @Override
    public FunctionLanguage getFunctionLanguage() {
        return functionLanguage;
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
                + "isGeneric='"
                + isGeneric()
                + "'}";
    }
}
