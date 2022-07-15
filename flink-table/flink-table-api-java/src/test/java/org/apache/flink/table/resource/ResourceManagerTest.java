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

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
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

    @ClassRule public static TemporaryFolder temporaryFolder = new TemporaryFolder();

    private static File udfJar;

    private ResourceManager resourceManager;

    @BeforeClass
    public static void prepare() throws Exception {
        udfJar =
                UserClassLoaderJarTestUtils.createJarFile(
                        temporaryFolder.newFolder("test-jar"),
                        "test-classloader-udf.jar",
                        GENERATED_LOWER_UDF_CLASS,
                        String.format(GENERATED_LOWER_UDF_CODE, GENERATED_LOWER_UDF_CLASS));
    }

    @Before
    public void before() {
        resourceManager =
                ResourceManager.createResourceManager(
                        new URL[0], getClass().getClassLoader(), new Configuration());
    }

    @Test
    public void testRegisterResource() throws Exception {
        URLClassLoader userClassLoader = resourceManager.getUserClassLoader();

        // test class loading before register resource
        CommonTestUtils.assertThrows(
                String.format("LowerUDF"),
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
        // test register non-exist file
        final String fileUri = temporaryFolder.getRoot().getPath() + Path.SEPARATOR + "test-file";

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
        final String jarDir = temporaryFolder.newFolder("test-jar-dir").getPath();

        CommonTestUtils.assertThrows(
                String.format(
                        "The registering jar resource [%s] must ends with '.jar' suffix.", jarDir),
                ValidationException.class,
                () -> {
                    resourceManager.registerJarResources(
                            Collections.singletonList(new ResourceUri(ResourceType.JAR, jarDir)));
                    return null;
                });

        // test register directory for jar resource
        final String jarUri = temporaryFolder.newFolder("test-jar.jar").getPath();

        CommonTestUtils.assertThrows(
                String.format(
                        "The registering jar resource [%s] is a directory that is not allowed.",
                        jarUri),
                ValidationException.class,
                () -> {
                    resourceManager.registerJarResources(
                            Collections.singletonList(new ResourceUri(ResourceType.JAR, jarUri)));
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

    @After
    public void after() throws Exception {
        resourceManager.close();

        // assert the sub dir deleted
        FileSystem fileSystem = FileSystem.getLocalFileSystem();
        assertFalse(fileSystem.exists(resourceManager.getLocalResourceDir()));
    }
}
