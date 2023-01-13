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

package org.apache.flink.table.catalog.hive.factories;

import org.apache.flink.connectors.hive.HiveTableFactory;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.FunctionLanguage;
import org.apache.flink.table.catalog.hive.client.HiveShim;
import org.apache.flink.table.factories.FunctionDefinitionFactory;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.functions.UserDefinedFunctionHelper;
import org.apache.flink.table.functions.hive.HiveFunctionWrapper;
import org.apache.flink.table.functions.hive.HiveGenericUDAF;
import org.apache.flink.table.functions.hive.HiveGenericUDF;
import org.apache.flink.table.functions.hive.HiveGenericUDTF;
import org.apache.flink.table.functions.hive.HiveSimpleUDF;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFResolver2;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** A factory to instantiate Hive UDFs as Flink UDFs. */
public class HiveFunctionDefinitionFactory implements FunctionDefinitionFactory {
    private static final Logger LOG = LoggerFactory.getLogger(HiveTableFactory.class);

    private final HiveShim hiveShim;

    public HiveFunctionDefinitionFactory(HiveShim hiveShim) {
        checkNotNull(hiveShim, "hiveShim cannot be null");
        this.hiveShim = hiveShim;
    }

    @Override
    public FunctionDefinition createFunctionDefinition(
            String name, CatalogFunction catalogFunction, Context context) {
        if (isFlinkFunction(catalogFunction, context.getClassLoader())) {
            return createFunctionDefinitionFromFlinkFunction(name, catalogFunction, context);
        }
        return createFunctionDefinitionFromHiveFunction(
                name, catalogFunction.getClassName(), context);
    }

    public FunctionDefinition createFunctionDefinitionFromFlinkFunction(
            String name, CatalogFunction catalogFunction, Context context) {
        return UserDefinedFunctionHelper.instantiateFunction(
                context.getClassLoader(), null, name, catalogFunction);
    }

    /**
     * Distinguish if the function is a Flink function.
     *
     * @return whether the function is a Flink function
     */
    private boolean isFlinkFunction(CatalogFunction catalogFunction, ClassLoader classLoader) {
        if (catalogFunction.getFunctionLanguage() == FunctionLanguage.PYTHON) {
            return true;
        }
        try {
            Class<?> c = Class.forName(catalogFunction.getClassName(), true, classLoader);
            if (UserDefinedFunction.class.isAssignableFrom(c)) {
                return true;
            }
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(
                    String.format("Can't resolve udf class %s", catalogFunction.getClassName()), e);
        }
        return false;
    }

    /**
     * Create a FunctionDefinition from a Hive function's class name. Called directly by {@link
     * org.apache.flink.table.module.hive.HiveModule}.
     */
    public FunctionDefinition createFunctionDefinitionFromHiveFunction(
            String name, String functionClassName, Context context) {
        Class<?> functionClz;
        try {
            functionClz = context.getClassLoader().loadClass(functionClassName);

            LOG.info("Successfully loaded Hive udf '{}' with class '{}'", name, functionClassName);
        } catch (ClassNotFoundException e) {
            throw new TableException(
                    String.format("Failed to initiate an instance of class %s.", functionClassName),
                    e);
        }

        if (UDF.class.isAssignableFrom(functionClz)) {
            LOG.info("Transforming Hive function '{}' into a HiveSimpleUDF", name);

            return new HiveSimpleUDF(new HiveFunctionWrapper<>(functionClz), hiveShim);
        } else if (GenericUDF.class.isAssignableFrom(functionClz)) {
            LOG.info("Transforming Hive function '{}' into a HiveGenericUDF", name);

            return new HiveGenericUDF(new HiveFunctionWrapper<>(functionClz), hiveShim);
        } else if (GenericUDTF.class.isAssignableFrom(functionClz)) {
            LOG.info("Transforming Hive function '{}' into a HiveGenericUDTF", name);
            return new HiveGenericUDTF(new HiveFunctionWrapper<>(functionClz), hiveShim);
        } else if (GenericUDAFResolver2.class.isAssignableFrom(functionClz)
                || GenericUDAFResolver.class.isAssignableFrom(functionClz)
                || UDAF.class.isAssignableFrom(functionClz)) {

            if (GenericUDAFResolver2.class.isAssignableFrom(functionClz)) {
                LOG.info(
                        "Transforming Hive function '{}' into a HiveGenericUDAF without UDAF bridging",
                        name);
                return new HiveGenericUDAF(
                        new HiveFunctionWrapper<>(functionClz), false, true, hiveShim);
            } else if (GenericUDAFResolver.class.isAssignableFrom(functionClz)) {
                LOG.info(
                        "Transforming Hive function '{}' into a HiveGenericUDAF without UDAF bridging",
                        name);
                return new HiveGenericUDAF(
                        new HiveFunctionWrapper<>(functionClz), false, false, hiveShim);
            } else {
                LOG.info(
                        "Transforming Hive function '{}' into a HiveGenericUDAF with UDAF bridging",
                        name);

                return new HiveGenericUDAF(
                        new HiveFunctionWrapper<>(functionClz), true, false, hiveShim);
            }
        } else {
            throw new IllegalArgumentException(
                    String.format(
                            "HiveFunctionDefinitionFactory cannot initiate FunctionDefinition for class %s",
                            functionClassName));
        }
    }
}
