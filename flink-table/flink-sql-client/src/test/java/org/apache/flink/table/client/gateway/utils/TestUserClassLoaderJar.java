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

package org.apache.flink.table.client.gateway.utils;

import org.apache.flink.util.FileUtils;

import javax.tools.DiagnosticCollector;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

/** Mainly used for testing classloading of UDF dependencies. */
public class TestUserClassLoaderJar {

    private static final String GENERATED_UDF_CLASS = "LowerUDF";

    private static final String GENERATED_UDF_CODE =
            "public class "
                    + GENERATED_UDF_CLASS
                    + " extends org.apache.flink.table.functions.ScalarFunction {\n"
                    + "  public String eval(String str) {\n"
                    + "    return str.toLowerCase();\n"
                    + "  }\n"
                    + "}\n";

    /** Pack the generated UDF class into a JAR and return the path of the JAR. */
    public static File createJarFile(File tmpDir, String jarName) throws IOException {
        // write class source code to file
        File javaFile = Paths.get(tmpDir.toString(), GENERATED_UDF_CLASS + ".java").toFile();
        //noinspection ResultOfMethodCallIgnored
        javaFile.createNewFile();
        FileUtils.writeFileUtf8(javaFile, GENERATED_UDF_CODE);

        // compile class source code
        DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<>();
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        StandardJavaFileManager fileManager =
                compiler.getStandardFileManager(diagnostics, null, null);
        Iterable<? extends JavaFileObject> compilationUnit =
                fileManager.getJavaFileObjectsFromFiles(Collections.singletonList(javaFile));
        JavaCompiler.CompilationTask task =
                compiler.getTask(
                        null,
                        fileManager,
                        diagnostics,
                        Collections.emptyList(),
                        null,
                        compilationUnit);
        task.call();

        // pack class file to jar
        File classFile = Paths.get(tmpDir.toString(), GENERATED_UDF_CLASS + ".class").toFile();
        File jarFile = Paths.get(tmpDir.toString(), jarName).toFile();
        JarOutputStream jos = new JarOutputStream(new FileOutputStream(jarFile));
        JarEntry jarEntry = new JarEntry(GENERATED_UDF_CLASS + ".class");
        jos.putNextEntry(jarEntry);
        byte[] classBytes = FileUtils.readAllBytes(classFile.toPath());
        jos.write(classBytes);
        jos.closeEntry();
        jos.close();

        return jarFile;
    }
}
