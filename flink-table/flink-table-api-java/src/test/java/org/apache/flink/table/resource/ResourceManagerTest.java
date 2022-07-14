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
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.UserClassLoaderJarTestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import static org.apache.flink.table.utils.UserDefinedFunctions.GENERATED_LOWER_UDF_CLASS;
import static org.apache.flink.table.utils.UserDefinedFunctions.GENERATED_LOWER_UDF_CODE;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/** Tests for {@link ResourceManager}. */
public class ResourceManagerTest {

    @TempDir private static File tempFolder;
    private static File udfJar;

    private ResourceManager resourceManager;

    @BeforeAll
    public static void prepare() throws Exception {
        udfJar =
                UserClassLoaderJarTestUtils.createJarFile(
                        tempFolder,
                        "test-classloader-udf.jar",
                        GENERATED_LOWER_UDF_CLASS,
                        String.format(GENERATED_LOWER_UDF_CODE, GENERATED_LOWER_UDF_CLASS));
    }

    @BeforeEach
    public void before() {
        resourceManager =
                ResourceManager.createResourceManager(
                        new URL[0], getClass().getClassLoader(), new Configuration());
    }

    @AfterEach
    public void after() throws Exception {
        resourceManager.close();
    }

    @Test
    public void testRegisterResource() throws Exception {
        URLClassLoader userClassLoader = resourceManager.getUserClassLoader();

        // test class loading before register resource
        CommonTestUtils.assertThrows(
                "LowerUDF",
                ClassNotFoundException.class,
                () -> Class.forName(GENERATED_LOWER_UDF_CLASS, false, userClassLoader));

        ResourceUri resourceUri = new ResourceUri(ResourceType.JAR, udfJar.getPath());
        // register the same jar repeatedly
        resourceManager.registerJarResources(Arrays.asList(resourceUri, resourceUri));

        // assert resource infos
        Map<ResourceUri, URL> expected =
                Collections.singletonMap(
                        resourceUri, resourceManager.getURLFromPath(new Path(udfJar.getPath())));

        assertEquals(expected, resourceManager.getResources());

        // test load class
        final Class<?> clazz1 = Class.forName(GENERATED_LOWER_UDF_CLASS, false, userClassLoader);
        final Class<?> clazz2 = Class.forName(GENERATED_LOWER_UDF_CLASS, false, userClassLoader);

        assertEquals(clazz1, clazz2);
    }

    @Test
    public void testRegisterResourceWithRelativePath() throws Exception {
        URLClassLoader userClassLoader = resourceManager.getUserClassLoader();

        // test class loading before register resource
        CommonTestUtils.assertThrows(
                "LowerUDF",
                ClassNotFoundException.class,
                () -> Class.forName(GENERATED_LOWER_UDF_CLASS, false, userClassLoader));

        ResourceUri resourceUri =
                new ResourceUri(
                        ResourceType.JAR,
                        new File(".")
                                .getCanonicalFile()
                                .toPath()
                                .relativize(udfJar.toPath())
                                .toString());
        // register jar
        resourceManager.registerJarResources(Collections.singletonList(resourceUri));

        // assert resource infos
        Map<ResourceUri, URL> expected =
                Collections.singletonMap(
                        new ResourceUri(ResourceType.JAR, udfJar.getPath()),
                        resourceManager.getURLFromPath(new Path(udfJar.getPath())));

        assertEquals(expected, resourceManager.getResources());

        // test load class
        final Class<?> clazz1 = Class.forName(GENERATED_LOWER_UDF_CLASS, false, userClassLoader);
        final Class<?> clazz2 = Class.forName(GENERATED_LOWER_UDF_CLASS, false, userClassLoader);

        assertEquals(clazz1, clazz2);
    }

    @Test
    public void testRegisterInvalidResource() throws Exception {
        final String fileUri = tempFolder.getAbsolutePath() + Path.SEPARATOR + "test-file";

        CommonTestUtils.assertThrows(
                String.format(
                        "Only support to register jar resource, resource info:\n %s.", fileUri),
                ValidationException.class,
                () -> {
                    resourceManager.registerJarResources(
                            Collections.singletonList(new ResourceUri(ResourceType.FILE, fileUri)));
                    return null;
                });

        // test register directory for jar resource
        final String jarDir = tempFolder.getPath();

        CommonTestUtils.assertThrows(
                String.format(
                        "The registering or unregistering jar resource [%s] must ends with '.jar' suffix.",
                        jarDir),
                ValidationException.class,
                () -> {
                    resourceManager.registerJarResources(
                            Collections.singletonList(new ResourceUri(ResourceType.JAR, jarDir)));
                    return null;
                });

        // test register directory for jar resource
        String jarPath =
                Files.createDirectory(Paths.get(tempFolder.getPath(), "test-jar.jar")).toString();

        CommonTestUtils.assertThrows(
                String.format(
                        "The registering or unregistering jar resource [%s] is a directory that is not allowed.",
                        jarPath),
                ValidationException.class,
                () -> {
                    resourceManager.registerJarResources(
                            Collections.singletonList(new ResourceUri(ResourceType.JAR, jarPath)));
                    return null;
                });
    }

    @Test
    public void testDownloadResource() throws Exception {
        Path srcPath = new Path(udfJar.getPath());
        // test download resource to local path
        URL localUrl = resourceManager.downloadResource(srcPath);

        byte[] expected = FileUtils.readAllBytes(udfJar.toPath());
        byte[] actual = FileUtils.readAllBytes(Paths.get(localUrl.toURI()));

        assertArrayEquals(expected, actual);
    }

    @Test
    public void testCloseResourceManagerCleanDownloadedResources() throws Exception {
        resourceManager.close();
        FileSystem fileSystem = FileSystem.getLocalFileSystem();
        assertFalse(fileSystem.exists(resourceManager.getLocalResourceDir()));
    }
}
