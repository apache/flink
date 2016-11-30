package org.apache.flink.contrib.streaming;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.util.ListCollector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.util.NamedThreadFactory;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * Created by vipulmodi on 11/30/16.
 */

//To return Futures to calling fucntion we can do OUT extends Future.
public class MultiThreadedFlatMapFunction<IN, OUT> extends RichFlatMapFunction<IN, OUT> {

    private static final long serialVersionUID;
    private ExecutorService flatMapExecutors;
    private Integer numberOfthreads;
    private List<Callable<ArrayList<OUT>>> callables;
    private List<Future<ArrayList<OUT>>> futures;
    static {
        serialVersionUID = 1L;
    }

    public FlatMapFunction flatMapFunction;

    public MultiThreadedFlatMapFunction(FlatMapFunction flatMapFunction, Integer numberOfthreads) {
        this.flatMapFunction = flatMapFunction;
        this.numberOfthreads = numberOfthreads;
        this.callables = new ArrayList<>();
        this.futures = new ArrayList<>();
    }

    // Understand side effects of making out final.
    @Override
    public void flatMap(final IN value, final Collector<OUT> out) throws Exception {
        // Figure out a better way to call
        /*callables.add(new Callable<ArrayList<OUT>>() {
            @Override
            public ArrayList<OUT> call() throws Exception {
                ArrayList<OUT> result = new ArrayList<OUT>();
                Collector<OUT> collector = new ListCollector<OUT>(result);
                flatMapFunction.flatMap(value, collector);
                return result;
            }
        });*/

        futures.add(flatMapExecutors.submit(new Callable<ArrayList<OUT>>() {
            @Override
            public ArrayList<OUT> call() throws Exception {
                ArrayList<OUT> result = new ArrayList<OUT>();
                Collector<OUT> collector = new ListCollector<OUT>(result);
                flatMapFunction.flatMap(value, collector);
                // Populate out synchronously.
                synchronized (out) {
                    for(OUT o: result) {
                        out.collect(o);
                    }
                }
                return result;
            }
        }));
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // Create a thread  pool.
        flatMapExecutors = Executors.newFixedThreadPool(
                numberOfthreads,
                new NamedThreadFactory("flatmap-executors", "-thread-")
        );
    }

    @Override
    public void close() throws Exception {
        super.close();
        // TODO:  Wait for futures for intead of doing invoke all.
        // Assumption all Callables or Futures can be held in memory.
        flatMapExecutors.invokeAll(callables);
        //shutdown thread pool.
        flatMapExecutors.shutdown();
        //wait for all threads to finish
        while(!flatMapExecutors.awaitTermination(1,TimeUnit.MINUTES));
    }
}
