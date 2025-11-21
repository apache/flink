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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DebugDslTest {

    @RegisterExtension
    private static final MiniClusterExtension MINI_CLUSTER_RESOURCE = new MiniClusterExtension();

    @Test
    public void testEventReferenceSimple() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Simple test: two events where second price > first price
        DataStream<StockEvent> input =
                env.fromData(
                        new StockEvent(
                                "AAPL", "TRADE", 100.0, 1000, DslTestDataSets.ts(0), "NASDAQ", 0.0),
                        new StockEvent(
                                "AAPL",
                                "TRADE",
                                105.0,
                                1100,
                                DslTestDataSets.ts(1),
                                "NASDAQ",
                                0.0));

        String dslExpression = "A(price > 0) -> B(price > A.price)";
        System.out.println("\n===== TEST: Event Reference =====");
        System.out.println("DSL: " + dslExpression);

        PatternStream<StockEvent> patternStream = DslCompiler.compile(dslExpression, input);

        DataStream<String> result =
                patternStream.select(
                        match -> {
                            double aPrice = match.get("A").get(0).getPrice();
                            double bPrice = match.get("B").get(0).getPrice();
                            return aPrice + "->" + bPrice;
                        });

        List<String> results = new ArrayList<>();
        result.executeAndCollect().forEachRemaining(results::add);

        System.out.println("Results: " + results);
        assertEquals(1, results.size(), "Should match one pattern");
        assertTrue(results.get(0).contains("100.0"));
        assertTrue(results.get(0).contains("105.0"));
    }

    @Test
    public void testTwoPatterns() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<StockEvent> input =
                env.fromData(
                        new StockEvent(
                                "AAPL", "TRADE", 100.0, 1000, DslTestDataSets.ts(0), "NASDAQ", 0.0),
                        new StockEvent(
                                "AAPL",
                                "TRADE",
                                105.0,
                                1100,
                                DslTestDataSets.ts(1),
                                "NASDAQ",
                                0.0));

        String dslExpression = "TRADE(price > 0) -> TRADE(price > 0)";
        System.out.println("\n===== TEST: Two TRADE Patterns =====");
        System.out.println("DSL: " + dslExpression);

        PatternStream<StockEvent> patternStream = DslCompiler.compile(dslExpression, input);

        DataStream<String> result =
                patternStream.select(
                        match -> {
                            String keys = String.join(",", match.keySet());
                            return "Keys: " + keys;
                        });

        List<String> results = new ArrayList<>();
        result.executeAndCollect().forEachRemaining(results::add);

        System.out.println("Results: " + results);
        System.out.println("Result count: " + results.size());
    }
}
