/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.examples.ml;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Skeleton for incremental machine learning algorithm consisting of a pre-computed model, which
 * gets updated for the new inputs and new input data for which the job provides predictions.
 *
 * <p>This may serve as a base of a number of algorithms, e.g. updating an incremental Alternating
 * Least Squares model while also providing the predictions.
 *
 * <p>This example shows how to use:
 *
 * <ul>
 *   <li>Connected streams
 *   <li>CoFunctions
 *   <li>Tuple data types
 * </ul>
 */
public class IncrementalLearningSkeleton {

    // *************************************************************************
    // PROGRAM
    // *************************************************************************

    public static void main(String[] args) throws Exception {

        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Integer> trainingData = env.addSource(new FiniteTrainingDataSource());
        DataStream<Integer> newData = env.addSource(new FiniteNewDataSource());

        // build new model on every second of new data
        DataStream<Double[]> model =
                trainingData
                        .assignTimestampsAndWatermarks(new LinearTimestamp())
                        .windowAll(TumblingEventTimeWindows.of(Time.milliseconds(5000)))
                        .apply(new PartialModelBuilder());

        // use partial model for newData
        DataStream<Integer> prediction = newData.connect(model).map(new Predictor());

        // emit result
        if (params.has("output")) {
            prediction.writeAsText(params.get("output"));
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            prediction.print();
        }

        // execute program
        env.execute("Streaming Incremental Learning");
    }

    // *************************************************************************
    // USER FUNCTIONS
    // *************************************************************************

    /**
     * Feeds new data for newData. By default it is implemented as constantly emitting the Integer 1
     * in a loop.
     */
    public static class FiniteNewDataSource implements SourceFunction<Integer> {
        private static final long serialVersionUID = 1L;
        private int counter;

        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            Thread.sleep(15);
            while (counter < 50) {
                ctx.collect(getNewData());
            }
        }

        @Override
        public void cancel() {
            // No cleanup needed
        }

        private Integer getNewData() throws InterruptedException {
            Thread.sleep(5);
            counter++;
            return 1;
        }
    }

    /**
     * Feeds new training data for the partial model builder. By default it is implemented as
     * constantly emitting the Integer 1 in a loop.
     */
    public static class FiniteTrainingDataSource implements SourceFunction<Integer> {
        private static final long serialVersionUID = 1L;
        private int counter = 0;

        @Override
        public void run(SourceContext<Integer> collector) throws Exception {
            while (counter < 8200) {
                collector.collect(getTrainingData());
            }
        }

        @Override
        public void cancel() {
            // No cleanup needed
        }

        private Integer getTrainingData() throws InterruptedException {
            counter++;
            return 1;
        }
    }

    private static class LinearTimestamp implements AssignerWithPunctuatedWatermarks<Integer> {
        private static final long serialVersionUID = 1L;

        private long counter = 0L;

        @Override
        public long extractTimestamp(Integer element, long previousElementTimestamp) {
            return counter += 10L;
        }

        @Override
        public Watermark checkAndGetNextWatermark(Integer lastElement, long extractedTimestamp) {
            return new Watermark(counter - 1);
        }
    }

    /** Builds up-to-date partial models on new training data. */
    public static class PartialModelBuilder
            implements AllWindowFunction<Integer, Double[], TimeWindow> {
        private static final long serialVersionUID = 1L;

        protected Double[] buildPartialModel(Iterable<Integer> values) {
            return new Double[] {1.};
        }

        @Override
        public void apply(TimeWindow window, Iterable<Integer> values, Collector<Double[]> out)
                throws Exception {
            out.collect(buildPartialModel(values));
        }
    }

    /**
     * Creates newData using the model produced in batch-processing and the up-to-date partial
     * model.
     *
     * <p>By default emits the Integer 0 for every newData and the Integer 1 for every model update.
     */
    public static class Predictor implements CoMapFunction<Integer, Double[], Integer> {
        private static final long serialVersionUID = 1L;

        Double[] batchModel = null;
        Double[] partialModel = null;

        @Override
        public Integer map1(Integer value) {
            // Return newData
            return predict(value);
        }

        @Override
        public Integer map2(Double[] value) {
            // Update model
            partialModel = value;
            batchModel = getBatchModel();
            return 1;
        }

        // pulls model built with batch-job on the old training data
        protected Double[] getBatchModel() {
            return new Double[] {0.};
        }

        // performs newData using the two models
        protected Integer predict(Integer inTuple) {
            return 0;
        }
    }
}
