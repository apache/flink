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

package org.apache.flink.yarn.security;

import org.apache.flink.yarn.configuration.YarnConfigOptions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/** Tests for {@link HadoopFSDelegationTokenProvider}. */
public class HadoopFSDelegationTokenProviderTest {

    public static final String HADOOP_SECURITY_AUTHENTICATION = "hadoop.security.authentication";

    @Test
    public void testDelegationTokensRequired() {
        HadoopFSDelegationTokenProvider provider = new HadoopFSDelegationTokenProvider();

        final org.apache.flink.configuration.Configuration flinkConf =
                new org.apache.flink.configuration.Configuration();
        final Configuration hadoopConf = new Configuration();
        assertEquals("simple", hadoopConf.get(HADOOP_SECURITY_AUTHENTICATION));
        assertFalse(
                "Hadoop FS delegation tokens are not required when authentication is simple",
                provider.delegationTokensRequired(flinkConf, hadoopConf));

        hadoopConf.set(HADOOP_SECURITY_AUTHENTICATION, "kerberos");
        // Set new hadoop conf to UGI to re-initialize it
        UserGroupInformation.setConfiguration(hadoopConf);
        assertTrue(
                "Hadoop FS delegation tokens are required when authentication is not simple",
                provider.delegationTokensRequired(flinkConf, hadoopConf));
    }

    @Test
    public void testGetFileSystemsToAccess() throws IOException {
        HadoopFSDelegationTokenProvider provider = new HadoopFSDelegationTokenProvider();
        final org.apache.flink.configuration.Configuration flinkConf =
                new org.apache.flink.configuration.Configuration();
        final Configuration hadoopConf = new Configuration();

        String defaultFSs = "hdfs://localhost:8020";
        hadoopConf.set("fs.defaultFS", defaultFSs);

        String additionalFs = "hdfs://localhost:8021";
        List<String> additionalFSs = Collections.singletonList(additionalFs);
        flinkConf.set(YarnConfigOptions.YARN_ACCESS, additionalFSs);

        Set<FileSystem> expected = new HashSet<>();
        expected.add(new Path(defaultFSs).getFileSystem(hadoopConf));
        expected.add(new Path(additionalFs).getFileSystem(hadoopConf));

        Set<FileSystem> fileSystems = provider.getFileSystemsToAccess(flinkConf, hadoopConf);

        assertThat(fileSystems, is(expected));
    }
}
