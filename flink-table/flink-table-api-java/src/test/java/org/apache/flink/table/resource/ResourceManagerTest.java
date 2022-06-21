/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.flink.table.resource;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.FlinkUserCodeClassLoaders;
import org.apache.flink.util.UserClassLoaderJarTestUtils;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Map;

import static org.apache.flink.util.FlinkUserCodeClassLoader.NOOP_EXCEPTION_HANDLER;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** Tests for {@link ResourceManager}. */
public class ResourceManagerTest {

    @ClassRule public static TemporaryFolder temporaryFolder = new TemporaryFolder();

    public static final String LOWER_UDF_CLASS = "LowerUDF";
    public static final String LOWER_UDF_CODE =
            "public class "
                    + LOWER_UDF_CLASS
                    + " extends org.apache.flink.table.functions.ScalarFunction {\n"
                    + "  public String eval(String str) {\n"
                    + "    return str.toLowerCase();\n"
                    + "  }\n"
                    + "}\n";

    private static File udfJar;

    @BeforeClass
    public static void prepare() throws Exception {
        udfJar =
                UserClassLoaderJarTestUtils.createJarFile(
                        temporaryFolder.newFolder("test-jar"),
                        "test-classloader-udf.jar",
                        LOWER_UDF_CLASS,
                        LOWER_UDF_CODE);
    }

    @Test
    public void testRegisterResource() throws Exception {
        ResourceManager resourceManager = createResourceManager(new URL[0]);
        URLClassLoader userClassLoader = resourceManager.getUserClassLoader();

        // test class loading before register resource
        CommonTestUtils.assertThrows(
                String.format("LowerUDF"),
                ClassNotFoundException.class,
                () -> Class.forName(LOWER_UDF_CLASS, false, userClassLoader));

        // register the same jar repeatedly
        resourceManager.registerResource(new ResourceUri(ResourceType.JAR, udfJar.getPath()));
        resourceManager.registerResource(new ResourceUri(ResourceType.JAR, udfJar.getPath()));

        // assert resource infos
        Map<ResourceUri, URL> expected =
                Collections.singletonMap(
                        new ResourceUri(ResourceType.JAR, udfJar.getPath()),
                        resourceManager.getURLFromPath(new Path(udfJar.getPath())));

        assertEquals(expected, resourceManager.getResources());

        // test load class
        final Class<?> clazz1 = Class.forName(LOWER_UDF_CLASS, false, userClassLoader);
        final Class<?> clazz2 = Class.forName(LOWER_UDF_CLASS, false, userClassLoader);

        assertEquals(clazz1, clazz2);

        resourceManager.close();
    }

    @Test
    public void testRegisterInvalidResource() throws Exception {
        ResourceManager resourceManager = createResourceManager(new URL[0]);

        // test register non-exist file
        final String fileUri =
                temporaryFolder.getRoot().getPath() + Path.SEPARATOR + "test-non-exist-file";

        CommonTestUtils.assertThrows(
                String.format("Resource [%s] not found.", fileUri),
                FileNotFoundException.class,
                () -> {
                    resourceManager.registerResource(new ResourceUri(ResourceType.FILE, fileUri));
                    return null;
                });

        // test register directory for jar resource
        final String jarUri = temporaryFolder.newFolder("test-jar-dir").getPath();

        CommonTestUtils.assertThrows(
                String.format(
                        "The resource [%s] is a directory, however, the directory is not allowed for registering resource.",
                        jarUri),
                IOException.class,
                () -> {
                    resourceManager.registerResource(new ResourceUri(ResourceType.JAR, jarUri));
                    return null;
                });

        resourceManager.close();
    }

    @Test
    public void testDownloadResource() throws Exception {
        Path srcPath = new Path(udfJar.getPath());
        ResourceManager resourceManager = createResourceManager(new URL[0]);

        // test download resource to local path
        URL localUrl = resourceManager.downloadResource(srcPath);

        byte[] expected = FileUtils.readAllBytes(udfJar.toPath());
        byte[] actual = FileUtils.readAllBytes(Paths.get(localUrl.toURI()));

        assertArrayEquals(expected, actual);

        resourceManager.close();
    }

    private ResourceManager createResourceManager(URL[] urls) {
        Configuration configuration = new Configuration();
        FlinkUserCodeClassLoaders.SafetyNetWrapperClassLoader mutableURLClassLoader =
                createClassLoader(configuration, urls, getClass().getClassLoader());
        return new ResourceManager(configuration, mutableURLClassLoader);
    }

    private FlinkUserCodeClassLoaders.SafetyNetWrapperClassLoader createClassLoader(
            Configuration configuration, URL[] urls, ClassLoader parentClassLoader) {
        final String[] alwaysParentFirstLoaderPatterns =
                CoreOptions.getParentFirstLoaderPatterns(configuration);

        // According to the specified class load order，child-first or parent-first
        // child-first: load from this classloader firstly, if not found, then from parent
        // parent-first: load from parent firstly, if not found, then from this classloader
        final String classLoaderResolveOrder =
                configuration.getString(CoreOptions.CLASSLOADER_RESOLVE_ORDER);
        FlinkUserCodeClassLoaders.ResolveOrder resolveOrder =
                FlinkUserCodeClassLoaders.ResolveOrder.fromString(classLoaderResolveOrder);
        return (FlinkUserCodeClassLoaders.SafetyNetWrapperClassLoader)
                FlinkUserCodeClassLoaders.create(
                        resolveOrder,
                        urls,
                        parentClassLoader,
                        alwaysParentFirstLoaderPatterns,
                        NOOP_EXCEPTION_HANDLER,
                        true);
    }
}
