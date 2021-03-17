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

package org.apache.flink.runtime.clusterframework.overlays;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.clusterframework.ContainerSpecification;

import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Test;

import java.security.PrivilegedAction;

import static org.junit.Assert.assertEquals;

public class HadoopUserOverlayTest extends ContainerOverlayTestBase {

    @Test
    public void testConfigure() throws Exception {

        final UserGroupInformation ugi = UserGroupInformation.createRemoteUser("test");

        HadoopUserOverlay overlay = new HadoopUserOverlay(ugi);

        ContainerSpecification spec = new ContainerSpecification();
        overlay.configure(spec);

        assertEquals(ugi.getUserName(), spec.getEnvironmentVariables().get("HADOOP_USER_NAME"));
    }

    @Test
    public void testNoConf() throws Exception {
        HadoopUserOverlay overlay = new HadoopUserOverlay(null);

        ContainerSpecification containerSpecification = new ContainerSpecification();
        overlay.configure(containerSpecification);
    }

    @Test
    public void testBuilderFromEnvironment() throws Exception {

        final Configuration conf = new Configuration();
        final UserGroupInformation ugi = UserGroupInformation.createRemoteUser("test");

        ugi.doAs(
                new PrivilegedAction<Object>() {
                    @Override
                    public Object run() {
                        try {
                            HadoopUserOverlay.Builder builder =
                                    HadoopUserOverlay.newBuilder().fromEnvironment(conf);
                            assertEquals(ugi, builder.ugi);
                            return null;
                        } catch (Exception ex) {
                            throw new AssertionError(ex);
                        }
                    }
                });
    }
}
