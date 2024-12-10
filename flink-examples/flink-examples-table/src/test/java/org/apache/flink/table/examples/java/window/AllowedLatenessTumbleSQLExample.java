package org.apache.flink.table.examples.java.window;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import static org.apache.flink.table.api.Expressions.$;

/**
 * Copyright © 2012-2019 Tencent BlueKing.
 * All Rights Reserved.
 * 蓝鲸智云 版权所有
 */
@SuppressWarnings("all")
public class AllowedLatenessTumbleSQLExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        //设置延迟计算
        tEnv.getConfig().set("table.exec.emit.allow-lateness", "1h");
        tEnv.getConfig().set("table.exec.emit.late-fire.enabled", "true");
        tEnv.getConfig().set("table.exec.emit.late-fire.delay", "10s");

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        dateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"));
        SimpleDateFormat utcFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        utcFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        DataStream<Tuple4<Long, String, Integer, Long>> ds = env
                .addSource(new SourceFunction<Tuple4<Long, String, Integer, Long>>() {
                    private static final long serialVersionUID = 1L;

                    private int c = 0;
                    long initTime = System.currentTimeMillis();
                    @Override
                    public void run(SourceContext<Tuple4<Long, String, Integer, Long>> ctx) throws Exception {
                        for (int i = 0; i <= 62; i++) {
                            ctx.collect(new Tuple4<>(2L, "rubber", 1, dateFormat.parse("2019-01-01 00:00:00").getTime() + i * 30 * 1000L));
                            Thread.sleep(100);
                        }
                        System.out.println("--delay data-----");
                        // 延迟的数据
                        for (int i = 0; i <= 600; i++) {
                            ctx.collect(new Tuple4<>(3L, "rubber", 1, dateFormat.parse("2019-01-01 00:00:00").getTime() + i * 30 * 1000L));
                            Thread.sleep(100);
                            if (System.currentTimeMillis() - initTime >= 10 * 1000) {
                                System.out.println("====================10 second====================");
                                initTime = System.currentTimeMillis();
                                // 特定延迟数据
                                ctx.collect(new Tuple4<>(4L, "rubber", 1, utcFormat.parse("2018-12-31 16:39:20").getTime() + 300 * 1000L * c));
                                System.out.println(new Tuple4<>(4L, "rubber", 1, utcFormat.format(new Date(utcFormat.parse("2018-12-31 16:39:20").getTime() + 300 * 1000L * c))));
                                c ++;
                            }
                        }
                        System.out.println("-----------------------------------------");
                        for (int i = 0; i < 100; i++) {
                            Thread.sleep( 1000);
                        }
                    }

                    @Override
                    public void cancel() {
                    }
                })
                .assignTimestampsAndWatermarks(new MyWatermarkExtractor());

        tEnv.createTemporaryView("Orders",
                tEnv.fromDataStream(ds, $("user"), $("product"), $("amount"), $("logtime").rowtime()));

        Table table = tEnv.sqlQuery("\n" +
                "SELECT\n" +
                "  user,\n" +
                "  TUMBLE_START(logtime, INTERVAL '5' minute) as wStart,\n" +
                "  SUM(amount)\n" +
                " FROM Orders\n" +
                " GROUP BY TUMBLE(logtime, INTERVAL '5' minute), user");

        DataStream<Row> result = tEnv.toAppendStream(table, Row.class);
        result.print();

        env.execute("test-for-combination");

    }


    private static class MyWatermarkExtractor implements AssignerWithPeriodicWatermarks<Tuple4<Long, String, Integer, Long>> {

        private static final long serialVersionUID = 1L;

        private Long currentTimestamp = Long.MIN_VALUE;

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            if (currentTimestamp == Long.MIN_VALUE) {
                return new Watermark(Long.MIN_VALUE);
            } else {
                return new Watermark(currentTimestamp - 1L);
            }
        }

        @Override
        public long extractTimestamp(Tuple4<Long, String, Integer, Long> element, long previousElementTimestamp) {
            this.currentTimestamp = Math.max(this.currentTimestamp, element.f3);
            // 返回记录的时间
            return element.f3;
        }
    }
}
