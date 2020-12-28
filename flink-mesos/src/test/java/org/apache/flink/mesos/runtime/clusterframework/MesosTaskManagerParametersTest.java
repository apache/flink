/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *	 http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.mesos.runtime.clusterframework;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.util.TestLogger;

import com.netflix.fenzo.ConstraintEvaluator;
import com.netflix.fenzo.functions.Func1;
import com.netflix.fenzo.plugins.HostAttrValueConstraint;
import org.apache.mesos.Protos;
import org.junit.Test;

import java.util.List;

import scala.Option;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/** Tests for the {@link MesosTaskManagerParameters}. */
public class MesosTaskManagerParametersTest extends TestLogger {
    private static final int TOTAL_PROCESS_MEMORY_MB = 1280;
    private static final MemorySize TOTAL_PROCESS_MEMORY_SIZE =
            MemorySize.ofMebiBytes(TOTAL_PROCESS_MEMORY_MB);

    @Test
    public void testBuildVolumes() throws Exception {
        List<Protos.Volume> vols;
        assertEquals(MesosTaskManagerParameters.buildVolumes(Option.<String>apply(null)).size(), 0);
        String spec1 =
                "/host/path:/container/path:RO,/container/path:ro,/host/path:/container/path,/container/path";
        vols = MesosTaskManagerParameters.buildVolumes(Option.<String>apply(spec1));
        assertEquals(vols.size(), 4);
        assertEquals("/container/path", vols.get(0).getContainerPath());
        assertEquals("/host/path", vols.get(0).getHostPath());
        assertEquals(Protos.Volume.Mode.RO, vols.get(0).getMode());
        assertEquals("/container/path", vols.get(1).getContainerPath());
        assertEquals(Protos.Volume.Mode.RO, vols.get(1).getMode());
        assertEquals("/container/path", vols.get(2).getContainerPath());
        assertEquals("/host/path", vols.get(2).getHostPath());
        assertEquals(Protos.Volume.Mode.RW, vols.get(2).getMode());
        assertEquals("/container/path", vols.get(3).getContainerPath());
        assertEquals(Protos.Volume.Mode.RW, vols.get(3).getMode());

        // should handle empty strings, but not error
        assertEquals(0, MesosTaskManagerParameters.buildVolumes(Option.<String>apply("")).size());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBuildVolumesBadMode() throws Exception {
        MesosTaskManagerParameters.buildVolumes(Option.<String>apply("/hp:/cp:RF"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBuildVolumesMalformed() throws Exception {
        MesosTaskManagerParameters.buildVolumes(Option.<String>apply("/hp:/cp:ro:extra"));
    }

    @Test
    public void testContainerVolumes() throws Exception {
        Configuration config = getConfiguration();
        config.setString(
                MesosTaskManagerParameters.MESOS_RM_CONTAINER_VOLUMES,
                "/host/path:/container/path:ro");

        MesosTaskManagerParameters params = MesosTaskManagerParameters.create(config);
        assertEquals(1, params.containerVolumes().size());
        assertEquals("/container/path", params.containerVolumes().get(0).getContainerPath());
        assertEquals("/host/path", params.containerVolumes().get(0).getHostPath());
        assertEquals(Protos.Volume.Mode.RO, params.containerVolumes().get(0).getMode());
    }

    @Test
    public void testContainerDockerParameter() throws Exception {
        Configuration config = getConfiguration();
        config.setString(
                MesosTaskManagerParameters.MESOS_RM_CONTAINER_DOCKER_PARAMETERS,
                "testKey=testValue");

        MesosTaskManagerParameters params = MesosTaskManagerParameters.create(config);
        assertEquals(params.dockerParameters().size(), 1);
        assertEquals(params.dockerParameters().get(0).getKey(), "testKey");
        assertEquals(params.dockerParameters().get(0).getValue(), "testValue");
    }

    @Test
    public void testContainerDockerParameters() throws Exception {
        Configuration config = getConfiguration();
        config.setString(
                MesosTaskManagerParameters.MESOS_RM_CONTAINER_DOCKER_PARAMETERS,
                "testKey1=testValue1,testKey2=testValue2,testParam3=key3=value3,testParam4=\"key4=value4\"");

        MesosTaskManagerParameters params = MesosTaskManagerParameters.create(config);
        assertEquals(params.dockerParameters().size(), 4);
        assertEquals(params.dockerParameters().get(0).getKey(), "testKey1");
        assertEquals(params.dockerParameters().get(0).getValue(), "testValue1");
        assertEquals(params.dockerParameters().get(1).getKey(), "testKey2");
        assertEquals(params.dockerParameters().get(1).getValue(), "testValue2");
        assertEquals(params.dockerParameters().get(2).getKey(), "testParam3");
        assertEquals(params.dockerParameters().get(2).getValue(), "key3=value3");
        assertEquals(params.dockerParameters().get(3).getKey(), "testParam4");
        assertEquals(params.dockerParameters().get(3).getValue(), "\"key4=value4\"");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testContainerDockerParametersMalformed() throws Exception {
        Configuration config = getConfiguration();
        config.setString(
                MesosTaskManagerParameters.MESOS_RM_CONTAINER_DOCKER_PARAMETERS, "badParam");
        MesosTaskManagerParameters params = MesosTaskManagerParameters.create(config);
    }

    @Test
    public void testUriParameters() throws Exception {
        Configuration config = getConfiguration();
        config.setString(
                MesosTaskManagerParameters.MESOS_TM_URIS,
                "file:///dev/null,http://localhost/test,  test_url ");

        MesosTaskManagerParameters params = MesosTaskManagerParameters.create(config);
        assertEquals(params.uris().size(), 3);
        assertEquals(params.uris().get(0), "file:///dev/null");
        assertEquals(params.uris().get(1), "http://localhost/test");
        assertEquals(params.uris().get(2), "test_url");
    }

    @Test
    public void testUriParametersDefault() throws Exception {
        Configuration config = getConfiguration();

        MesosTaskManagerParameters params = MesosTaskManagerParameters.create(config);
        assertEquals(params.uris().size(), 0);
    }

    @Test
    public void testForcePullImageTrue() {
        Configuration config = getConfiguration();
        config.setBoolean(
                MesosTaskManagerParameters.MESOS_RM_CONTAINER_DOCKER_FORCE_PULL_IMAGE, true);

        MesosTaskManagerParameters params = MesosTaskManagerParameters.create(config);
        assertEquals(params.dockerForcePullImage(), true);
    }

    @Test
    public void testForcePullImageFalse() {
        Configuration config = getConfiguration();
        config.setBoolean(
                MesosTaskManagerParameters.MESOS_RM_CONTAINER_DOCKER_FORCE_PULL_IMAGE, false);

        MesosTaskManagerParameters params = MesosTaskManagerParameters.create(config);
        assertEquals(params.dockerForcePullImage(), false);
    }

    @Test
    public void testForcePullImageDefault() {
        Configuration config = getConfiguration();

        MesosTaskManagerParameters params = MesosTaskManagerParameters.create(config);
        assertEquals(params.dockerForcePullImage(), false);
    }

    @Test
    public void givenTwoConstraintsInConfigShouldBeParsed() throws Exception {

        MesosTaskManagerParameters mesosTaskManagerParameters =
                MesosTaskManagerParameters.create(
                        withHardHostAttrConstraintConfiguration("cluster:foo,az:eu-west-1"));
        assertThat(mesosTaskManagerParameters.constraints().size(), is(2));
        ConstraintEvaluator firstConstraintEvaluator =
                new HostAttrValueConstraint(
                        "cluster",
                        new Func1<String, String>() {
                            @Override
                            public String call(String s) {
                                return "foo";
                            }
                        });
        ConstraintEvaluator secondConstraintEvaluator =
                new HostAttrValueConstraint(
                        "az",
                        new Func1<String, String>() {
                            @Override
                            public String call(String s) {
                                return "foo";
                            }
                        });
        assertThat(
                mesosTaskManagerParameters.constraints().get(0).getName(),
                is(firstConstraintEvaluator.getName()));
        assertThat(
                mesosTaskManagerParameters.constraints().get(1).getName(),
                is(secondConstraintEvaluator.getName()));
    }

    @Test
    public void givenOneConstraintInConfigShouldBeParsed() throws Exception {

        MesosTaskManagerParameters mesosTaskManagerParameters =
                MesosTaskManagerParameters.create(
                        withHardHostAttrConstraintConfiguration("cluster:foo"));
        assertThat(mesosTaskManagerParameters.constraints().size(), is(1));
        ConstraintEvaluator firstConstraintEvaluator =
                new HostAttrValueConstraint(
                        "cluster",
                        new Func1<String, String>() {
                            @Override
                            public String call(String s) {
                                return "foo";
                            }
                        });
        assertThat(
                mesosTaskManagerParameters.constraints().get(0).getName(),
                is(firstConstraintEvaluator.getName()));
    }

    @Test
    public void givenEmptyConstraintInConfigShouldBeParsed() throws Exception {

        MesosTaskManagerParameters mesosTaskManagerParameters =
                MesosTaskManagerParameters.create(withHardHostAttrConstraintConfiguration(""));
        assertThat(mesosTaskManagerParameters.constraints().size(), is(0));
    }

    @Test
    public void givenInvalidConstraintInConfigShouldBeParsed() throws Exception {

        MesosTaskManagerParameters mesosTaskManagerParameters =
                MesosTaskManagerParameters.create(withHardHostAttrConstraintConfiguration(",:,"));
        assertThat(mesosTaskManagerParameters.constraints().size(), is(0));
    }

    @Test(expected = IllegalConfigurationException.class)
    public void testNegativeNumberOfGPUs() throws Exception {
        MesosTaskManagerParameters.create(withGPUConfiguration(-1));
    }

    @Test
    public void testConfigCpuCores() {
        Configuration config = getConfiguration();
        config.setDouble(TaskManagerOptions.CPU_CORES, 1.5);
        config.setDouble(MesosTaskManagerParameters.MESOS_RM_TASKS_CPUS, 2.5);
        config.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, 3);
        MesosTaskManagerParameters mesosTaskManagerParameters =
                MesosTaskManagerParameters.create(config);
        assertThat(mesosTaskManagerParameters.cpus(), is(1.5));
    }

    @Test
    public void testLegacyConfigCpuCores() {
        Configuration config = getConfiguration();
        config.setDouble(MesosTaskManagerParameters.MESOS_RM_TASKS_CPUS, 1.5);
        config.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, 3);
        MesosTaskManagerParameters mesosTaskManagerParameters =
                MesosTaskManagerParameters.create(config);
        assertThat(mesosTaskManagerParameters.cpus(), is(1.5));
    }

    @Test
    public void testConfigNoCpuCores() {
        Configuration conf = new Configuration();
        conf.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, 3);
        conf.set(TaskManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.parse("1g"));
        MesosTaskManagerParameters mesosTaskManagerParameters =
                MesosTaskManagerParameters.create(conf);
        assertThat(mesosTaskManagerParameters.cpus(), is(3.0));
    }

    @Test
    public void testUnifiedTotalProcessMemoryConfiguration() {
        assertTotalProcessMemory(MesosTaskManagerParameters.create(getConfiguration()));
    }

    private void assertTotalProcessMemory(MesosTaskManagerParameters mesosTaskManagerParameters) {
        assertThat(
                mesosTaskManagerParameters
                        .containeredParameters()
                        .getTaskExecutorProcessSpec()
                        .getTotalProcessMemorySize(),
                is(TOTAL_PROCESS_MEMORY_SIZE));
    }

    private static Configuration getConfiguration() {
        Configuration config = new Configuration();
        config.set(TaskManagerOptions.TOTAL_PROCESS_MEMORY, TOTAL_PROCESS_MEMORY_SIZE);
        return config;
    }

    private static Configuration withGPUConfiguration(int gpus) {
        Configuration config = getConfiguration();
        config.setInteger(MesosTaskManagerParameters.MESOS_RM_TASKS_GPUS, gpus);
        return config;
    }

    private static Configuration withHardHostAttrConstraintConfiguration(
            final String configuration) {
        Configuration config = getConfiguration();
        config.setString(MesosTaskManagerParameters.MESOS_CONSTRAINTS_HARD_HOSTATTR, configuration);
        return config;
    }
}
