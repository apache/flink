package org.apache.flink.cep.dsl;

import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.dsl.api.DslCompiler;
import org.apache.flink.cep.dsl.model.StockEvent;
import org.apache.flink.cep.dsl.util.DslTestDataSets;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.List;

public class DebugEventRefTest {

    @RegisterExtension
    private static final MiniClusterExtension MINI_CLUSTER_RESOURCE = new MiniClusterExtension();

    @Test
    public void debugSimpleReference() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<StockEvent> input =
                env.fromData(
                        new StockEvent(
                                "AAPL", "TRADE", 105.0, 1100, DslTestDataSets.ts(0), "NASDAQ", 0.0),
                        new StockEvent(
                                "AAPL",
                                "TRADE",
                                110.0,
                                1500,
                                DslTestDataSets.ts(1),
                                "NASDAQ",
                                0.0));

        // Test 1: Without reference - should work
        System.out.println("\n=== Test 1: Without event reference ===");
        String dsl1 = "START(price > 100) -> INCREASE(price > 105)";
        System.out.println("DSL: " + dsl1);

        PatternStream<StockEvent> ps1 = DslCompiler.compile(dsl1, input);
        List<String> results1 = new ArrayList<>();
        ps1.select(
                        m ->
                                m.get("START").get(0).getPrice()
                                        + "->"
                                        + m.get("INCREASE").get(0).getPrice())
                .executeAndCollect()
                .forEachRemaining(results1::add);

        System.out.println("Results: " + results1);
        System.out.println("Match count: " + results1.size());

        // Test 2: With constant reference - simplified
        System.out.println("\n=== Test 2: With event reference (constant comparison) ===");
        String dsl2 = "START(price > 0) -> INCREASE(price > 100)";
        System.out.println("DSL: " + dsl2);

        PatternStream<StockEvent> ps2 = DslCompiler.compile(dsl2, input);
        List<String> results2 = new ArrayList<>();
        ps2.select(
                        m ->
                                m.get("START").get(0).getPrice()
                                        + "->"
                                        + m.get("INCREASE").get(0).getPrice())
                .executeAndCollect()
                .forEachRemaining(results2::add);

        System.out.println("Results: " + results2);
        System.out.println("Match count: " + results2.size());

        // Test 3: With event reference
        System.out.println("\n=== Test 3: With event reference ===");
        String dsl3 = "START(price > 100) -> INCREASE(price > START.price)";
        System.out.println("DSL: " + dsl3);

        PatternStream<StockEvent> ps3 = DslCompiler.compile(dsl3, input);
        List<String> results3 = new ArrayList<>();
        ps3.select(
                        m ->
                                m.get("START").get(0).getPrice()
                                        + "->"
                                        + m.get("INCREASE").get(0).getPrice())
                .executeAndCollect()
                .forEachRemaining(results3::add);

        System.out.println("Results: " + results3);
        System.out.println("Match count: " + results3.size());
    }
}
