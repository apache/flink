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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.flink.table.utils.UserDefinedFunctions.GENERATED_LOWER_UDF_CLASS;
import static org.apache.flink.table.utils.UserDefinedFunctions.GENERATED_LOWER_UDF_CODE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;

/** Tests for {@link ResourceManager}. */
public class ResourceManagerTest {

    @TempDir private static File tempFolder;
    private static File udfJar;

    private static File file;

    private ResourceManager resourceManager;

    @BeforeAll
    public static void prepare() throws Exception {
        udfJar =
                UserClassLoaderJarTestUtils.createJarFile(
                        tempFolder,
                        "test-classloader-udf.jar",
                        GENERATED_LOWER_UDF_CLASS,
                        String.format(GENERATED_LOWER_UDF_CODE, GENERATED_LOWER_UDF_CLASS));
        file = File.createTempFile("ResourceManagerTest", ".txt", tempFolder);
        FileUtils.writeFileUtf8(file, "Hello World");
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
        FileSystem.initialize(new Configuration(), null);
    }

    @Test
    public void testRegisterJarResource() throws Exception {
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
    public void testRegisterFileResource() throws Exception {
        ResourceUri normalizedResource =
                new ResourceUri(
                        ResourceType.FILE,
                        resourceManager.getURLFromPath(new Path(file.getPath())).getPath());

        // register file resource, uri is formatted with "file" scheme prefix
        String localFilePath =
                resourceManager.registerFileResource(
                        new ResourceUri(ResourceType.FILE, "file://" + file.getPath()));
        assertEquals(file.getPath(), localFilePath);
        Map<ResourceUri, URL> actualResource =
                Collections.singletonMap(
                        normalizedResource,
                        resourceManager.getURLFromPath(new Path(localFilePath)));
        assertThat(resourceManager.getResources()).containsExactlyEntriesOf(actualResource);

        // register the same file resource repeatedly, but without scheme
        assertThat(
                        resourceManager.registerFileResource(
                                new ResourceUri(ResourceType.FILE, file.getPath())))
                .isEqualTo(localFilePath);
        assertThat(resourceManager.getResources()).containsExactlyEntriesOf(actualResource);

        // register the same file resource repeatedly, use relative path as uri
        assertThat(
                        resourceManager.registerFileResource(
                                new ResourceUri(
                                        ResourceType.FILE,
                                        new File(".")
                                                .getCanonicalFile()
                                                .toPath()
                                                .relativize(file.toPath())
                                                .toString())))
                .isEqualTo(localFilePath);
        assertThat(resourceManager.getResources()).containsExactlyEntriesOf(actualResource);
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
    public void testRegisterInvalidJarResource() throws Exception {
        final String fileUri = file.getPath();

        CommonTestUtils.assertThrows(
                String.format(
                        "Expect the resource type to be jar, but encounter a resource [%s] with type %s.",
                        fileUri, ResourceType.FILE.name().toLowerCase()),
                ValidationException.class,
                () -> {
                    resourceManager.registerJarResources(
                            Collections.singletonList(new ResourceUri(ResourceType.FILE, fileUri)));
                    return null;
                });

        // test register jar resource with invalid suffix
        CommonTestUtils.assertThrows(
                String.format(
                        "The registering or unregistering jar resource [%s] must ends with '.jar' suffix.",
                        fileUri),
                ValidationException.class,
                () -> {
                    resourceManager.registerJarResources(
                            Collections.singletonList(new ResourceUri(ResourceType.JAR, fileUri)));
                    return null;
                });

        // test register directory for jar resource
        final String jarDir = tempFolder.getPath();

        CommonTestUtils.assertThrows(
                String.format(
                        "The registering or unregistering jar resource [%s] is a directory that is not allowed.",
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

    @MethodSource("provideResource")
    @ParameterizedTest
    public void testDownloadResource(String pathString, boolean executable) throws Exception {
        Path srcPath = new Path(pathString);
        // test download resource to local path
        URL localUrl = resourceManager.downloadResource(srcPath, executable);

        byte[] expected = FileUtils.readAllBytes(Paths.get(pathString));
        byte[] actual = FileUtils.readAllBytes(Paths.get(localUrl.toURI()));

        assertArrayEquals(expected, actual);
    }

    @CsvSource({
        "file://path/to/file,hdfs://foo/bar:9000/,false",
        "/path/to/file,file://foo/bar/,false",
        "../path/to/file,file://foo/bar/,false",
        "/path/to/file,hdfs://foo/bar:9000/,true",
        "../path/to/file,hdfs://foo/bar:9000/,true",
        "hdfs://path/to/file,file://foo/bar/,true",
    })
    @ParameterizedTest
    public void testIsRemotePath(String pathString, String defaultFsScheme, boolean remote) {
        Configuration conf = new Configuration();
        conf.set(CoreOptions.DEFAULT_FILESYSTEM_SCHEME, defaultFsScheme);
        FileSystem.initialize(conf, null);
        assertThat(resourceManager.isRemotePath(new Path(pathString))).isEqualTo(remote);
    }

    @Test
    public void testSyncFileResource() throws Exception {
        String targetUri = tempFolder.getAbsolutePath() + "foo/bar.txt";
        ResourceUri resource = new ResourceUri(ResourceType.FILE, targetUri);
        resourceManager.syncFileResource(
                resource,
                path -> {
                    try {
                        FileUtils.copy(new Path(file.getPath()), new Path(path), false);
                    } catch (IOException e) {
                        fail("Test failed.", e);
                    }
                });
        assertThat(FileUtils.readFileUtf8(new File(targetUri))).isEqualTo("Hello World");

        // test overwrite
        resourceManager.syncFileResource(
                resource,
                path -> {
                    try {
                        Files.write(
                                new File(targetUri).toPath(),
                                "Bye Bye".getBytes(StandardCharsets.UTF_8),
                                StandardOpenOption.CREATE,
                                StandardOpenOption.TRUNCATE_EXISTING,
                                StandardOpenOption.WRITE);
                    } catch (IOException e) {
                        fail("Test failed.", e);
                    }
                });
        assertThat(FileUtils.readFileUtf8(new File(targetUri))).isEqualTo("Bye Bye");
    }

    @Test
    public void testCloseResourceManagerCleanDownloadedResources() throws Exception {
        resourceManager.close();
        FileSystem fileSystem = FileSystem.getLocalFileSystem();
        assertFalse(fileSystem.exists(resourceManager.getLocalResourceDir()));
    }

    public static Stream<Arguments> provideResource() {
        return Stream.of(Arguments.of(udfJar.getPath(), true), Arguments.of(file.getPath(), false));
    }
}
