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

package org.apache.flink.client.program;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.streaming.api.datastream.DataStream;

import org.junit.Test;

import java.io.File;
import java.net.URISyntaxException;

import static org.apache.flink.client.program.PackagedProgramUtils.resolveURI;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * Tests {@link PackagedProgramUtils}.
 *
 * <p>See also {@link PackagedProgramUtilsPipelineTest} for tests that need to test behaviour of
 * {@link DataStream} and {@link DataSet} programs.
 */
public class PackagedProgramUtilsTest {

    @Test
    public void testResolveURI() throws URISyntaxException {
        final String relativeFile = "path/of/user.jar";
        assertThat(resolveURI(relativeFile).getScheme(), is("file"));
        assertThat(
                resolveURI(relativeFile).getPath(),
                is(new File(System.getProperty("user.dir"), relativeFile).getAbsolutePath()));

        final String absoluteFile = "/path/of/user.jar";
        assertThat(resolveURI(relativeFile).getScheme(), is("file"));
        assertThat(resolveURI(absoluteFile).getPath(), is(absoluteFile));

        final String fileSchemaFile = "file:///path/of/user.jar";
        assertThat(resolveURI(fileSchemaFile).getScheme(), is("file"));
        assertThat(resolveURI(fileSchemaFile).toString(), is(fileSchemaFile));

        final String localSchemaFile = "local:///path/of/user.jar";
        assertThat(resolveURI(localSchemaFile).getScheme(), is("local"));
        assertThat(resolveURI(localSchemaFile).toString(), is(localSchemaFile));
    }
}
