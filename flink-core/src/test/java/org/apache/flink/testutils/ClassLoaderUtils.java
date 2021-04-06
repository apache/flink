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

package org.apache.flink.testutils;

import javax.annotation.Nullable;
import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/** Utilities to create class loaders. */
public class ClassLoaderUtils {
    public static URLClassLoader compileAndLoadJava(File root, String filename, String source)
            throws IOException {
        return withRoot(root).addClass(filename.replaceAll("\\.java", ""), source).build();
    }

    private static URLClassLoader createClassLoader(File root) throws MalformedURLException {
        return new URLClassLoader(
                new URL[] {root.toURI().toURL()}, Thread.currentThread().getContextClassLoader());
    }

    private static void writeAndCompile(File root, String filename, String source)
            throws IOException {
        File file = writeSourceFile(root, filename, source);

        compileClass(file);
    }

    private static File writeSourceFile(File root, String filename, String source)
            throws IOException {
        File file = new File(root, filename);
        file.getParentFile().mkdirs();
        FileWriter fileWriter = new FileWriter(file);

        fileWriter.write(source);
        fileWriter.close();

        return file;
    }

    public static ClassLoaderBuilder withRoot(File root) {
        return new ClassLoaderBuilder(root);
    }

    private static int compileClass(File sourceFile) {
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        return compiler.run(null, null, null, "-proc:none", sourceFile.getPath());
    }

    public static URL[] getClasspathURLs() {
        final String[] cp = System.getProperty("java.class.path").split(File.pathSeparator);

        return Arrays.stream(cp)
                .filter(str -> !str.isEmpty())
                .map(ClassLoaderUtils::parse)
                .toArray(URL[]::new);
    }

    private static URL parse(String fileName) {
        try {
            return new File(fileName).toURI().toURL();
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }

    public static class ClassLoaderBuilder {

        private final File root;
        private final Map<String, String> classes;
        private final Map<String, String> resources;

        private ClassLoaderBuilder(File root) {
            this.root = root;
            this.classes = new HashMap<>();
            this.resources = new HashMap<>();
        }

        public ClassLoaderBuilder addResource(String targetPath, String resource) {
            String oldValue = resources.putIfAbsent(targetPath, resource);

            if (oldValue != null) {
                throw new RuntimeException(
                        String.format("Resource with path %s already registered.", resource));
            }

            return this;
        }

        public ClassLoaderBuilder addClass(String className, String source) {
            String oldValue = classes.putIfAbsent(className, source);

            if (oldValue != null) {
                throw new RuntimeException(
                        String.format("Class with name %s already registered.", className));
            }

            return this;
        }

        public URLClassLoader build() throws IOException {
            for (Map.Entry<String, String> classInfo : classes.entrySet()) {
                writeAndCompile(root, createFileName(classInfo.getKey()), classInfo.getValue());
            }

            for (Map.Entry<String, String> resource : resources.entrySet()) {
                writeSourceFile(root, resource.getKey(), resource.getValue());
            }

            return createClassLoader(root);
        }

        private String createFileName(String className) {
            return className + ".java";
        }
    }
    // ------------------------------------------------------------------------
    //  Testing of objects not in the application class loader
    // ------------------------------------------------------------------------

    /**
     * A new object and the corresponding ClassLoader for that object, as returned by {@link
     * #createSerializableObjectFromNewClassLoader()} or {@link
     * #createExceptionObjectFromNewClassLoader()}.
     */
    public static final class ObjectAndClassLoader<T> {

        private final T object;
        private final ClassLoader classLoader;

        private ObjectAndClassLoader(T object, ClassLoader classLoader) {
            this.object = object;
            this.classLoader = classLoader;
        }

        public ClassLoader getClassLoader() {
            return classLoader;
        }

        public T getObject() {
            return object;
        }
    }

    /**
     * Creates a new ClassLoader and a new {@link Serializable} class inside that ClassLoader. This
     * is useful when unit testing the class loading behavior of code, and needing a class that is
     * outside the system class path.
     *
     * <p>NOTE: Even though this method may throw IOExceptions, we do not declare those and rather
     * wrap them in Runtime Exceptions. While this is generally discouraged, we do this here because
     * it is merely a test utility and not production code, and it makes it easier to use this
     * method during the initialization of variables and especially static variables.
     */
    public static ObjectAndClassLoader<Serializable> createSerializableObjectFromNewClassLoader() {

        final String classSource =
                ""
                        + "import java.io.Serializable;"
                        + "import java.util.Random;"
                        + "public class TestSerializable implements Serializable {"
                        + "  private static final long serialVersionUID = -3L;"
                        + "  private final long random;"
                        + "  public TestSerializable() {"
                        + "    random = new Random().nextLong();"
                        + "  }"
                        + "  public boolean equals(Object o) {"
                        + "    if (this == o) { return true; }"
                        + "    if ((o == null) || (getClass() != o.getClass())) { return false; }"
                        + "    TestSerializable that = (TestSerializable) o;"
                        + "    return random == random;"
                        + "  }"
                        + "  public int hashCode() {"
                        + "    return (int)(random ^ random >>> 32);"
                        + "  }"
                        + "  public String toString() {"
                        + "    return \"TestSerializable{random=\" + random + '}';"
                        + "  }"
                        + "}";

        return createObjectFromNewClassLoader("TestSerializable", Serializable.class, classSource);
    }

    /**
     * Creates a new ClassLoader and a new {@link Exception} class inside that ClassLoader. This is
     * useful when unit testing the class loading behavior of code, and needing a class that is
     * outside the system class path.
     *
     * <p>NOTE: Even though this method may throw IOExceptions, we do not declare those and rather
     * wrap them in Runtime Exceptions. While this is generally discouraged, we do this here because
     * it is merely a test utility and not production code, and it makes it easier to use this
     * method during the initialization of variables and especially static variables.
     */
    public static ObjectAndClassLoader<Exception> createExceptionObjectFromNewClassLoader() {

        return createObjectFromNewClassLoader(
                "TestExceptionForSerialization",
                Exception.class,
                "public class TestExceptionForSerialization extends java.lang.Exception {}");
    }

    private static <T> ObjectAndClassLoader<T> createObjectFromNewClassLoader(
            String testClassName, Class<T> testClass, String source) {
        final Path classDirPath =
                new File(System.getProperty("java.io.tmpdir"), UUID.randomUUID().toString())
                        .toPath();

        URLClassLoader classLoader = null;
        try {
            Files.createDirectories(classDirPath);
            classLoader = compileAndLoadJava(classDirPath.toFile(), testClassName, source);

            final Class<?> clazz = classLoader.loadClass(testClassName);
            final T object = clazz.asSubclass(testClass).getDeclaredConstructor().newInstance();

            return new ObjectAndClassLoader<>(object, classLoader);
        } catch (Exception e) {
            throw new RuntimeException("Cannot create test class outside system class path", e);
        } finally {
            // we clean up eagerly, because it is fine to delete the class file once the class is
            // loaded
            // and we have no later life cycle hook here to do the cleanup
            tryClose(classLoader);
            tryDeleteDirectoryRecursively(classDirPath);
        }
    }

    // ------------------------------------------------------------------------
    //  miscellaneous utils
    // ------------------------------------------------------------------------

    private static void tryClose(@Nullable AutoCloseable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (Exception ignored) {
            }
        }
    }

    private static void tryDeleteDirectoryRecursively(Path directory) {
        final SimpleFileVisitor<Path> deletingVisitor =
                new SimpleFileVisitor<Path>() {
                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                            throws IOException {
                        Files.delete(file);
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult postVisitDirectory(Path dir, IOException exc)
                            throws IOException {
                        Files.delete(dir);
                        return FileVisitResult.CONTINUE;
                    }
                };

        try {
            Files.walkFileTree(directory, deletingVisitor);
        } catch (Exception ignored) {
        }
    }
}
