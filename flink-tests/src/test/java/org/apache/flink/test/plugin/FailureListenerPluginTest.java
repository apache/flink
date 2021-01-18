/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.test.plugin;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.failurelistener.FailureListener;
import org.apache.flink.core.failurelistener.FailureListenerFactory;
import org.apache.flink.core.plugin.DefaultPluginManager;
import org.apache.flink.core.plugin.DirectoryBasedPluginFinder;
import org.apache.flink.core.plugin.PluginDescriptor;
import org.apache.flink.core.plugin.PluginFinder;
import org.apache.flink.core.plugin.PluginManager;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.runtime.failurelistener.FailureListenerUtils;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableMap;
import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Test for {@link org.apache.flink.core.failurelistener.FailureListenerFactory}. */
public class FailureListenerPluginTest extends PluginTestBase {

    @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

    public Path pluginRootFolderPath;

    private Collection<PluginDescriptor> descriptors;

    @Before
    public void setup() throws Exception {
        /*
         * We setup a plugin directory hierarchy and utilize DirectoryBasedPluginFinder to create the
         * descriptors:
         *
         * <pre>
         * tmp/plugins-root/
         *          |-------------failure-listener/
         *          |             |-plugin-failure-listener.jar
         * </pre>
         */
        final File pluginRootFolder = temporaryFolder.newFolder();
        this.pluginRootFolderPath = pluginRootFolder.toPath();
        final File pluginListenerFolder = new File(pluginRootFolder, "failure-listener");
        Preconditions.checkState(pluginListenerFolder.mkdirs());
        Files.copy(
                locateJarFile(PLUGIN_FAILURE_LISTENER).toPath(),
                Paths.get(pluginListenerFolder.toString(), PLUGIN_FAILURE_LISTENER));
        final PluginFinder descriptorsFactory =
                new DirectoryBasedPluginFinder(pluginRootFolderPath);
        descriptors = descriptorsFactory.findPlugins();
        Preconditions.checkState(descriptors.size() == 1);
    }

    @Test
    public void testPluginManagerLoadFailureListener() {
        String[] parentPatterns = {FailureListener.class.getName()};
        final PluginManager pluginManager =
                new DefaultPluginManager(descriptors, PARENT_CLASS_LOADER, parentPatterns);
        final List<FailureListenerFactory> serviceImplList =
                Lists.newArrayList(pluginManager.load(FailureListenerFactory.class));
        Assert.assertEquals(1, serviceImplList.size());
    }

    @Test
    public void testFailureListenerFactoryLoadPlugin() {

        final Map<String, String> envVariables =
                ImmutableMap.of(
                        ConfigConstants.ENV_FLINK_PLUGINS_DIR,
                        new File(pluginRootFolderPath.toUri()).getAbsolutePath());
        CommonTestUtils.setEnv(envVariables);

        Set<FailureListener> failureListeners =
                FailureListenerUtils.getFailureListeners(
                        new Configuration(),
                        JobID.generate(),
                        "test-job",
                        UnregisteredMetricGroups.createUnregisteredJobManagerJobMetricGroup());

        Assert.assertEquals(2, failureListeners.size());
    }
}
