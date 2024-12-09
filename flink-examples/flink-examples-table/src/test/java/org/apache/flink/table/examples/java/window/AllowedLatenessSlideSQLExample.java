package org.apache.flink.table.examples.java.window;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;

import java.text.SimpleDateFormat;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.flink.table.api.Expressions.$;

/**
 * Copyright © 2012-2019 Tencent BlueKing.
 * All Rights Reserved.
 * 蓝鲸智云 版权所有
 */
@SuppressWarnings("all")
public class AllowedLatenessSlideSQLExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        //设置延迟计算
        tEnv.getConfig().set("table.exec.emit.allow-lateness", "48h");
        tEnv.getConfig().set("table.exec.emit.late-fire.enabled", "true");
        tEnv.getConfig().set("table.exec.emit.late-fire.delay", "30s");

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        dateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"));


        DataStream<Tuple4<Long, String, Integer, Long>> ds = env
                .addSource(new SourceFunction<Tuple4<Long, String, Integer, Long>>() {
                    @Override
                    public void run(SourceContext<Tuple4<Long, String, Integer, Long>> ctx) throws Exception {
                        ctx.collect(new Tuple4<>(getId(), "rubber", 3, dateFormat.parse("2019-03-20 00:10:00").getTime()));
                        //ctx.collect(new Tuple4<>(getId(), "rubber", 3, dateFormat.parse("2019-03-20 00:20:10").getTime()));
                        //ctx.collect(new Tuple4<>(getId(), "rubber", 3, dateFormat.parse("2019-03-20 00:20:11").getTime()));
                        ctx.collect(new Tuple4<>(getId(), "rubber", 3, dateFormat.parse("2019-03-20 00:50:10").getTime()));

                        Thread.sleep(10 *1000);
                        ctx.collect(new Tuple4<>(getId(), "rubber", 4, dateFormat.parse("2019-03-20 00:22:14").getTime()));

                        //ctx.collect(new Tuple4<>(getId(), "rubber", 3, dateFormat.parse("2019-03-20 00:30:12").getTime()));
                        //ctx.collect(new Tuple4<>(getId(), "rubber", 3, dateFormat.parse("2019-03-20 00:50:10").getTime()));
                        System.out.println("-----------------------------------------");
                        for (int i = 0; i < 45; i++) {
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
                "  HOP_START(logtime, INTERVAL '3' minute, INTERVAL '10' MINUTE) as wStart,\n" +
                "  HOP_END(logtime, INTERVAL '3' minute, INTERVAL '10' MINUTE) as wStart,\n" +
                "  SUM(amount)\n" +
                " FROM Orders\n" +
                " GROUP BY HOP(logtime, INTERVAL '3' minute, INTERVAL '10' MINUTE), user");

        DataStream<Row> result = tEnv.toAppendStream(table, Row.class);
        result.print();

        env.execute("test for slide");
    }

    private static AtomicLong id = new AtomicLong(10000);
    private static long getId(){
        return id.get();
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
                return new Watermark(currentTimestamp - 30 * 1000L);
            }
        }

        @Override
        public long extractTimestamp(Tuple4<Long, String, Integer, Long> element, long previousElementTimestamp) {
            this.currentTimestamp = element.f3;
            return this.currentTimestamp;
        }
    }
}
