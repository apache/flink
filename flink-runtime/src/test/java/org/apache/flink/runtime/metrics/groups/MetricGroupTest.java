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

package org.apache.flink.runtime.metrics.groups;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.MetricRegistryConfiguration;
import org.apache.flink.runtime.metrics.MetricRegistryImpl;
import org.apache.flink.runtime.metrics.MetricRegistryTestUtils;
import org.apache.flink.runtime.metrics.NoOpMetricRegistry;
import org.apache.flink.runtime.metrics.ReporterSetup;
import org.apache.flink.runtime.metrics.dump.QueryScopeInfo;
import org.apache.flink.runtime.metrics.scope.ScopeFormat;
import org.apache.flink.runtime.metrics.util.DummyCharacterFilter;
import org.apache.flink.runtime.metrics.util.TestReporter;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createExecutionAttemptId;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/** Tests for the {@link MetricGroup}. */
public class MetricGroupTest {

    private static final MetricRegistryConfiguration defaultMetricRegistryConfiguration =
            MetricRegistryTestUtils.defaultMetricRegistryConfiguration();

    private MetricRegistryImpl registry;

    private final MetricRegistryImpl exceptionOnRegister = new ExceptionOnRegisterRegistry();

    @BeforeEach
    void createRegistry() {
        this.registry = new MetricRegistryImpl(defaultMetricRegistryConfiguration);
    }

    @AfterEach
    void shutdownRegistry() throws Exception {
        this.registry.closeAsync().get();
        this.registry = null;
    }

    @Test
    void sameGroupOnNameCollision() {
        GenericMetricGroup group =
                new GenericMetricGroup(
                        registry, new DummyAbstractMetricGroup(registry), "somegroup");

        String groupName = "sometestname";
        MetricGroup subgroup1 = group.addGroup(groupName);
        MetricGroup subgroup2 = group.addGroup(groupName);

        assertThat(subgroup1).isSameAs(subgroup2);
    }

    /** Verifies the basic behavior when defining user-defined variables. */
    @Test
    void testUserDefinedVariable() {
        MetricRegistry registry = NoOpMetricRegistry.INSTANCE;
        GenericMetricGroup root =
                new GenericMetricGroup(registry, new DummyAbstractMetricGroup(registry), "root");

        String key = "key";
        String value = "value";
        MetricGroup group = root.addGroup(key, value);

        String variableValue = group.getAllVariables().get(ScopeFormat.asVariable("key"));
        assertThat(variableValue).isEqualTo(value);

        String identifier = group.getMetricIdentifier("metric");
        assertThat(identifier)
                .withFailMessage("Key is missing from metric identifier.")
                .contains("key");
        assertThat(identifier)
                .withFailMessage("Value is missing from metric identifier.")
                .contains("value");

        String logicalScope =
                ((AbstractMetricGroup) group).getLogicalScope(new DummyCharacterFilter());
        assertThat(logicalScope)
                .withFailMessage("Key is missing from logical scope.")
                .contains(key);
        assertThat(logicalScope)
                .withFailMessage("Value is present in logical scope.")
                .doesNotContain(value);
    }

    /**
     * Verifies that calling {@link MetricGroup#addGroup(String, String)} on a {@link
     * GenericKeyMetricGroup} goes through the generic code path.
     */
    @Test
    void testUserDefinedVariableOnKeyGroup() {
        MetricRegistry registry = NoOpMetricRegistry.INSTANCE;
        GenericMetricGroup root =
                new GenericMetricGroup(registry, new DummyAbstractMetricGroup(registry), "root");

        String key1 = "key1";
        String value1 = "value1";
        root.addGroup(key1, value1);

        String key2 = "key2";
        String value2 = "value2";
        MetricGroup group = root.addGroup(key1).addGroup(key2, value2);

        String variableValue = group.getAllVariables().get("value2");
        assertThat(variableValue).isNull();

        String identifier = group.getMetricIdentifier("metric");
        assertThat(identifier)
                .withFailMessage("Key1 is missing from metric identifier.")
                .contains("key1");
        assertThat(identifier)
                .withFailMessage("Key2 is missing from metric identifier.")
                .contains("key2");
        assertThat(identifier)
                .withFailMessage("Value2 is missing from metric identifier.")
                .contains("value2");

        String logicalScope =
                ((AbstractMetricGroup) group).getLogicalScope(new DummyCharacterFilter());
        assertThat(logicalScope)
                .withFailMessage("Key1 is missing from logical scope.")
                .contains(key1);
        assertThat(logicalScope)
                .withFailMessage("Key2 is missing from logical scope.")
                .contains(key2);
        assertThat(logicalScope)
                .withFailMessage("Value2 is missing from logical scope.")
                .contains(value2);
    }

    /**
     * Verifies that calling {@link MetricGroup#addGroup(String, String)} if a generic group with
     * the key name already exists goes through the generic code path.
     */
    @Test
    void testNameCollisionForKeyAfterGenericGroup() {
        MetricRegistry registry = NoOpMetricRegistry.INSTANCE;
        GenericMetricGroup root =
                new GenericMetricGroup(registry, new DummyAbstractMetricGroup(registry), "root");

        String key = "key";
        String value = "value";

        root.addGroup(key);
        MetricGroup group = root.addGroup(key, value);

        String variableValue = group.getAllVariables().get(ScopeFormat.asVariable("key"));
        assertThat(variableValue).isNull();

        String identifier = group.getMetricIdentifier("metric");
        assertThat(identifier)
                .withFailMessage("Key is missing from metric identifier.")
                .contains("key");
        assertThat(identifier)
                .withFailMessage("Value is missing from metric identifier.")
                .contains("value");

        String logicalScope =
                ((AbstractMetricGroup) group).getLogicalScope(new DummyCharacterFilter());
        assertThat(logicalScope)
                .withFailMessage("Key is missing from logical scope.")
                .contains(key);
        assertThat(logicalScope)
                .withFailMessage("Value is missing from logical scope.")
                .contains(value);
    }

    /**
     * Verifies that calling {@link MetricGroup#addGroup(String, String)} if a generic group with
     * the key and value name already exists goes through the generic code path.
     */
    @Test
    void testNameCollisionForKeyAndValueAfterGenericGroup() {
        MetricRegistry registry = NoOpMetricRegistry.INSTANCE;
        GenericMetricGroup root =
                new GenericMetricGroup(registry, new DummyAbstractMetricGroup(registry), "root");

        String key = "key";
        String value = "value";

        root.addGroup(key).addGroup(value);
        MetricGroup group = root.addGroup(key, value);

        String variableValue = group.getAllVariables().get(ScopeFormat.asVariable("key"));
        assertThat(variableValue).isNull();

        String identifier = group.getMetricIdentifier("metric");
        assertThat(identifier)
                .withFailMessage("Key is missing from metric identifier.")
                .contains("key");
        assertThat(identifier)
                .withFailMessage("Value is missing from metric identifier.")
                .contains("value");

        String logicalScope =
                ((AbstractMetricGroup) group).getLogicalScope(new DummyCharacterFilter());
        assertThat(logicalScope)
                .withFailMessage("Key is missing from logical scope.")
                .contains(key);
        assertThat(logicalScope)
                .withFailMessage("Value is missing from logical scope.")
                .contains(value);
    }

    /**
     * Verifies that existing key/value groups are returned when calling {@link
     * MetricGroup#addGroup(String)}.
     */
    @Test
    void testNameCollisionAfterKeyValueGroup() {
        MetricRegistry registry = NoOpMetricRegistry.INSTANCE;
        GenericMetricGroup root =
                new GenericMetricGroup(registry, new DummyAbstractMetricGroup(registry), "root");

        String key = "key";
        String value = "value";

        root.addGroup(key, value);
        MetricGroup group = root.addGroup(key).addGroup(value);

        String variableValue = group.getAllVariables().get(ScopeFormat.asVariable("key"));
        assertThat(variableValue).isEqualTo(value);

        String identifier = group.getMetricIdentifier("metric");
        assertThat(identifier)
                .withFailMessage("Key is missing from metric identifier.")
                .contains("key");
        assertThat(identifier)
                .withFailMessage("Value is missing from metric identifier.")
                .contains("value");

        String logicalScope =
                ((AbstractMetricGroup) group).getLogicalScope(new DummyCharacterFilter());
        assertThat(logicalScope)
                .withFailMessage("Key is missing from logical scope.")
                .contains(key);
        assertThat(logicalScope)
                .withFailMessage("Value is present in logical scope.")
                .doesNotContain(value);
    }

    /**
     * Verifies that calling {@link AbstractMetricGroup#getLogicalScope(CharacterFilter, char, int)}
     * on {@link GenericValueMetricGroup} should ignore value as well.
     */
    @Test
    void testLogicalScopeShouldIgnoreValueGroupName() throws Exception {
        Configuration config = new Configuration();

        MetricRegistryImpl registry =
                new MetricRegistryImpl(
                        MetricRegistryTestUtils.defaultMetricRegistryConfiguration(),
                        Arrays.asList(ReporterSetup.forReporter("test", new TestReporter())));
        try {
            GenericMetricGroup root =
                    new GenericMetricGroup(
                            registry, new DummyAbstractMetricGroup(registry), "root");

            String key = "key";
            String value = "value";

            MetricGroup group = root.addGroup(key, value);

            String logicalScope =
                    ((AbstractMetricGroup) group)
                            .getLogicalScope(
                                    new DummyCharacterFilter(), registry.getDelimiter(), 0);
            assertThat(logicalScope)
                    .withFailMessage("Key is missing from logical scope.")
                    .contains(key);
            assertThat(logicalScope)
                    .withFailMessage("Value is present in logical scope.")
                    .doesNotContain(value);
        } finally {
            registry.closeAsync().get();
        }
    }

    @Test
    void closedGroupDoesNotRegisterMetrics() {
        GenericMetricGroup group =
                new GenericMetricGroup(
                        exceptionOnRegister,
                        new DummyAbstractMetricGroup(exceptionOnRegister),
                        "testgroup");
        assertThat(group.isClosed()).isFalse();

        group.close();
        assertThat(group.isClosed()).isTrue();

        // these will fail is the registration is propagated
        group.counter("testcounter");
        group.gauge(
                "testgauge",
                new Gauge<Object>() {
                    @Override
                    public Object getValue() {
                        return null;
                    }
                });
    }

    @Test
    void closedGroupCreatesClosedGroups() {
        GenericMetricGroup group =
                new GenericMetricGroup(
                        exceptionOnRegister,
                        new DummyAbstractMetricGroup(exceptionOnRegister),
                        "testgroup");
        assertThat(group.isClosed()).isFalse();

        group.close();
        assertThat(group.isClosed()).isTrue();

        AbstractMetricGroup subgroup = (AbstractMetricGroup) group.addGroup("test subgroup");
        assertThat(subgroup.isClosed()).isTrue();
    }

    @Test
    void addClosedGroupReturnsNewGroupInstance() {
        GenericMetricGroup mainGroup =
                new GenericMetricGroup(
                        exceptionOnRegister,
                        new DummyAbstractMetricGroup(exceptionOnRegister),
                        "mainGroup");

        AbstractMetricGroup<?> subGroup = (AbstractMetricGroup<?>) mainGroup.addGroup("subGroup");

        assertThat(subGroup.isClosed()).isFalse();

        subGroup.close();
        assertThat(subGroup.isClosed()).isTrue();

        AbstractMetricGroup<?> newSubGroupWithSameNameAsClosedGroup =
                (AbstractMetricGroup<?>) mainGroup.addGroup("subGroup");
        assertThat(newSubGroupWithSameNameAsClosedGroup.isClosed())
                .withFailMessage("The new subgroup should not be closed")
                .isFalse();
        assertThat(subGroup.isClosed())
                .withFailMessage("The old sub group is not modified")
                .isTrue();
    }

    @Test
    void tolerateMetricNameCollisions() {
        final String name = "abctestname";
        GenericMetricGroup group =
                new GenericMetricGroup(
                        registry, new DummyAbstractMetricGroup(registry), "testgroup");

        assertThat(group.counter(name)).isNotNull();
        assertThat(group.counter(name)).isNotNull();
    }

    @Test
    void tolerateMetricAndGroupNameCollisions() {
        final String name = "abctestname";
        GenericMetricGroup group =
                new GenericMetricGroup(
                        registry, new DummyAbstractMetricGroup(registry), "testgroup");

        assertThat(group.addGroup(name)).isNotNull();
        assertThat(group.counter(name)).isNotNull();
    }

    @Test
    void testCreateQueryServiceMetricInfo() {
        JobID jid = new JobID();
        JobVertexID vid = new JobVertexID();
        ExecutionAttemptID eid = createExecutionAttemptId(vid, 4, 5);
        MetricRegistryImpl registry = new MetricRegistryImpl(defaultMetricRegistryConfiguration);
        TaskManagerMetricGroup tm =
                TaskManagerMetricGroup.createTaskManagerMetricGroup(
                        registry, "host", new ResourceID("id"));

        TaskMetricGroup task = tm.addJob(jid, "jobname").addTask(eid, "taskName");
        GenericMetricGroup userGroup1 = new GenericMetricGroup(registry, task, "hello");
        GenericMetricGroup userGroup2 = new GenericMetricGroup(registry, userGroup1, "world");

        QueryScopeInfo.TaskQueryScopeInfo info1 =
                (QueryScopeInfo.TaskQueryScopeInfo)
                        userGroup1.createQueryServiceMetricInfo(new DummyCharacterFilter());
        assertThat(info1.scope).isEqualTo("hello");
        assertThat(info1.jobID).isEqualTo(jid.toString());
        assertThat(info1.vertexID).isEqualTo(vid.toString());
        assertThat(info1.subtaskIndex).isEqualTo(4);

        QueryScopeInfo.TaskQueryScopeInfo info2 =
                (QueryScopeInfo.TaskQueryScopeInfo)
                        userGroup2.createQueryServiceMetricInfo(new DummyCharacterFilter());
        assertThat(info2.scope).isEqualTo("hello.world");
        assertThat(info2.jobID).isEqualTo(jid.toString());
        assertThat(info2.vertexID).isEqualTo(vid.toString());
        assertThat(info2.subtaskIndex).isEqualTo(4);
    }

    // ------------------------------------------------------------------------

    private static class ExceptionOnRegisterRegistry extends MetricRegistryImpl {

        public ExceptionOnRegisterRegistry() {
            super(defaultMetricRegistryConfiguration);
        }

        @Override
        public void register(Metric metric, String name, AbstractMetricGroup parent) {
            fail("Metric should never be registered");
        }

        @Override
        public void unregister(Metric metric, String name, AbstractMetricGroup parent) {
            fail("Metric should never be un-registered");
        }
    }

    // ------------------------------------------------------------------------

    /**
     * A dummy {@link AbstractMetricGroup} to be used when a group is required as an argument but
     * not actually used.
     */
    public static class DummyAbstractMetricGroup extends AbstractMetricGroup {

        public DummyAbstractMetricGroup(MetricRegistry registry) {
            super(registry, new String[0], null);
        }

        @Override
        protected QueryScopeInfo createQueryServiceMetricInfo(CharacterFilter filter) {
            return null;
        }

        @Override
        protected String getGroupName(CharacterFilter filter) {
            return "foo";
        }

        @Override
        protected void addMetric(String name, Metric metric) {}

        @Override
        public MetricGroup addGroup(String name) {
            return new DummyAbstractMetricGroup(registry);
        }
    }
}
