package org.apache.flink.cep;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.cep.listen.CepListener;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.RichIterativeCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.ArrayList;
import java.util.Map;

/**
 * @title:
 * @author: zhangyf
 * @date: 2023/7/19 16:09
 */
public class CEPTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //		数据源
        SingleOutputStreamOperator<Tuple3<String, Long, String>> source =
                env.fromElements(
                                new Tuple3<String, Long, String>("a", 1000000001000L, "22"),
                                new Tuple3<String, Long, String>("b", 1000000002000L, "23"),
                                new Tuple3<String, Long, String>("c", 1000000003000L, "23"),
                                new Tuple3<String, Long, String>("d", 1000000003000L, "23"),
                                new Tuple3<String, Long, String>("change", 1000000003001L, "23"),
                                new Tuple3<String, Long, String>("e", 1000000004000L, "24"),
                                new Tuple3<String, Long, String>("f", 1000000005000L, "23"),
                                new Tuple3<String, Long, String>("g", 1000000006000L, "23"))
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy
                                        .<Tuple3<String, Long, String>>forMonotonousTimestamps()
                                        .withTimestampAssigner(
                                                new SerializableTimestampAssigner<
                                                        Tuple3<String, Long, String>>() {
                                                    @Override
                                                    public long extractTimestamp(
                                                            Tuple3<String, Long, String> element,
                                                            long recordTimestamp) {
                                                        return element.f1;
                                                    }
                                                }));

        Pattern<Tuple3<String, Long, String>, Tuple3<String, Long, String>> initPattern =
                Pattern.<Tuple3<String, Long, String>>begin("start")
                        .where(
                                new RichIterativeCondition<Tuple3<String, Long, String>>() {
                                    @Override
                                    public boolean filter(
                                            Tuple3<String, Long, String> value,
                                            IterativeCondition.Context<Tuple3<String, Long, String>>
                                                    ctx)
                                            throws Exception {
                                        return value.f0.equals("a");
                                    }
                                })
                        .next("middle")
                        .where(
                                new RichIterativeCondition<Tuple3<String, Long, String>>() {
                                    @Override
                                    public boolean filter(
                                            Tuple3<String, Long, String> value,
                                            IterativeCondition.Context<Tuple3<String, Long, String>>
                                                    ctx)
                                            throws Exception {
                                        return value.f0.equals("b");
                                    }
                                })
                        .within(Time.minutes(5));

        PatternStream patternStream = CEP.pattern(source, initPattern);

        PatternStream patternstream =
                patternStream
                        //	Pattern 更新逻辑
                        .registerListener(
                        new CepListener<Tuple3<String, Long, String>>() {
                            @Override
                            public Boolean needChange(Tuple3<String, Long, String> element) {
                                return element.f0.equals("change");
                            }

                            @Override
                            public Pattern returnPattern(Tuple3<String, Long, String> flagElement) {
                                System.out.println("接收到更新数据:" + flagElement.toString() + "感知到切换逻辑");
                                Pattern<Tuple3<String, Long, String>, ?> pattern =
                                        Pattern.<Tuple3<String, Long, String>>begin("start")
                                                .where(
                                                        new RichIterativeCondition<
                                                                Tuple3<String, Long, String>>() {
                                                            @Override
                                                            public boolean filter(
                                                                    Tuple3<String, Long, String>
                                                                            value,
                                                                    IterativeCondition.Context<
                                                                                    Tuple3<
                                                                                            String,
                                                                                            Long,
                                                                                            String>>
                                                                            ctx)
                                                                    throws Exception {
                                                                return value.f0.equals("e");
                                                            }
                                                        })
                                                .next("middle")
                                                .where(
                                                        new RichIterativeCondition<
                                                                Tuple3<String, Long, String>>() {
                                                            @Override
                                                            public boolean filter(
                                                                    Tuple3<String, Long, String>
                                                                            value,
                                                                    IterativeCondition.Context<
                                                                                    Tuple3<
                                                                                            String,
                                                                                            Long,
                                                                                            String>>
                                                                            ctx)
                                                                    throws Exception {
                                                                return value.f0.equals("f");
                                                            }
                                                        })
                                                .next("end")
                                                .where(
                                                        new RichIterativeCondition<
                                                                Tuple3<String, Long, String>>() {
                                                            @Override
                                                            public boolean filter(
                                                                    Tuple3<String, Long, String>
                                                                            value,
                                                                    IterativeCondition.Context<
                                                                                    Tuple3<
                                                                                            String,
                                                                                            Long,
                                                                                            String>>
                                                                            ctx)
                                                                    throws Exception {
                                                                return value.f0.equals("g");
                                                            }
                                                        })
                                                .within(Time.minutes(5));
                                return pattern;
                            }
                        });

        patternstream
                .select(
                        new RichPatternSelectFunction<
                                Tuple3<String, Long, String>, Tuple3<String, String, String>>() {
                            @Override
                            public Tuple3 select(Map pattern) throws Exception {
                                String start =
                                        pattern.containsKey("start")
                                                ? ((ArrayList) pattern.get("start"))
                                                        .get(0)
                                                        .toString()
                                                : "";
                                String middle =
                                        pattern.containsKey("middle")
                                                ? ((ArrayList) pattern.get("middle"))
                                                        .get(0)
                                                        .toString()
                                                : "";
                                String end =
                                        pattern.containsKey("end")
                                                ? ((ArrayList) pattern.get("end")).get(0).toString()
                                                : "";
                                System.out.println("end" + end);
                                return new Tuple3<String, String, String>(start, middle, end);
                            }
                        })
                .print();

        env.execute("dynamic cep pattern");
    }
}
