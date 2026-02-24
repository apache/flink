package org.apache.flink.table.test.join;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.Test;
import org.apache.notflink.TestClass;

import java.util.ArrayList;
import java.util.Collections;

import static org.apache.flink.api.common.typeinfo.Types.INT;
import static org.apache.flink.api.common.typeinfo.Types.ROW_NAMED;
import static org.apache.flink.table.api.Expressions.$;

/**
 * Verifies that {@link org.apache.flink.table.runtime.operators.join.adaptive.AdaptiveJoinOperatorFactory}
 * can deserialize data
 */
public class JoinDeserializationITCase extends AbstractTestBase {

    private <T> void runTest(
            String jobName,
            T value,
            TypeInformation<T> valueTypeInfo) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        Table table1 = tEnv.fromDataStream(
                env.fromData(Row.of(1))
                        .returns(ROW_NAMED(new String[]{"id"}, INT))
        );

        Table table2 = tEnv.fromDataStream(
                env.fromData(Row.of(1, value))
                        .returns(ROW_NAMED(new String[]{"id2", "value"}, INT, valueTypeInfo))
        );

        tEnv.toDataStream(table1.leftOuterJoin(table2, $("id").isEqual($("id2"))))
                .sinkTo(new DiscardingSink<>());

        JobClient jobClient = env.executeAsync(jobName);

        CommonTestUtils.waitForJobStatus(jobClient, Collections.singletonList(JobStatus.FINISHED));
    }

    @Test
    void testFlinkPojo() throws Exception {
        runTest(
                "testFlinkPojo",
                new FlinkNamespaceTestClass(),
                new PojoTypeInfo<>(FlinkNamespaceTestClass.class, new ArrayList<>())
        );
    }

    @Test
    void testUserPojo() throws Exception {
        runTest(
                "testUserPojo",
                new TestClass(),
                new PojoTypeInfo<>(TestClass.class, new ArrayList<>())
        );
    }

    public static class FlinkNamespaceTestClass {
    }
}
