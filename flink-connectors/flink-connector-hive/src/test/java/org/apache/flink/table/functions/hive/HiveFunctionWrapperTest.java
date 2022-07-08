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

package org.apache.flink.table.functions.hive;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.util.FlinkUserCodeClassLoaders;
import org.apache.flink.util.UserClassLoaderJarTestUtils;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDFMacro;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.net.URL;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link HiveFunctionWrapper}. */
public class HiveFunctionWrapperTest {

    @TempDir private static File tempFolder;

    private static final Random random = new Random();
    private static String udfClassName;
    private static File udfJar;

    @BeforeAll
    static void before() throws Exception {
        udfClassName = "MyToLower" + random.nextInt(50);
        String udfCode =
                "public class "
                        + "%s"
                        + " extends org.apache.flink.table.functions.ScalarFunction {\n"
                        + "  public String eval(String str) {\n"
                        + "    return str.toLowerCase();\n"
                        + "  }\n"
                        + "}\n";
        udfJar =
                UserClassLoaderJarTestUtils.createJarFile(
                        tempFolder,
                        "test-classloader-udf.jar",
                        udfClassName,
                        String.format(udfCode, udfClassName));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testDeserializeUDF() throws Exception {
        // test deserialize udf
        GenericUDFMacro udfMacro = new GenericUDFMacro();
        HiveFunctionWrapper<GenericUDFMacro> functionWrapper =
                new HiveFunctionWrapper<>(GenericUDFMacro.class, udfMacro);
        GenericUDFMacro deserializeUdfMacro = functionWrapper.createFunction();
        assertThat(deserializeUdfMacro.getClass().getName())
                .isEqualTo(GenericUDFMacro.class.getName());

        // test deserialize udf loaded by user code class loader instead of current thread class
        // loader
        ClassLoader userClassLoader =
                FlinkUserCodeClassLoaders.create(
                        new URL[] {udfJar.toURI().toURL()},
                        getClass().getClassLoader(),
                        new Configuration());
        Class<ScalarFunction> udfClass =
                (Class<ScalarFunction>) userClassLoader.loadClass(udfClassName);
        ScalarFunction udf = udfClass.newInstance();
        HiveFunctionWrapper<ScalarFunction> functionWrapper1 =
                new HiveFunctionWrapper<>(udfClass, udf);
        ScalarFunction deserializedUdf = functionWrapper1.createFunction();
        assertThat(deserializedUdf.getClass().getName()).isEqualTo(udfClassName);
    }
}
