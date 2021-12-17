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

package org.apache.flink.streaming.examples.statemachine.generator;

import org.apache.flink.streaming.examples.statemachine.event.Event;
import org.apache.flink.util.Collector;

import java.io.IOException;

/**
 * Base for standalone generators that use the state machine to create event sequences and push them
 * for example into Kafka.
 */
public class StandaloneThreadedGenerator {

    public static void runGenerator(Collector<Event>[] collectors) throws IOException {

        final GeneratorThread[] threads = new GeneratorThread[collectors.length];
        final int range = Integer.MAX_VALUE / collectors.length;

        // create the generator threads
        for (int i = 0; i < threads.length; i++) {
            int min = range * i;
            int max = min + range;
            GeneratorThread thread = new GeneratorThread(collectors[i], min, max);
            threads[i] = thread;
            thread.setName("Generator " + i);
        }

        long delay = 2L;
        int nextErroneous = 0;
        boolean running = true;

        for (GeneratorThread t : threads) {
            t.setDelay(delay);
            t.start();
        }

        final ThroughputLogger throughputLogger = new ThroughputLogger(threads);
        throughputLogger.start();

        System.out.println("Commands:");
        System.out.println(" -> q : Quit");
        System.out.println(" -> + : increase latency");
        System.out.println(" -> - : decrease latency");
        System.out.println(" -> e : inject invalid state transition");

        // input loop

        while (running) {
            final int next = System.in.read();

            switch (next) {
                case 'q':
                    System.out.println("Quitting...");
                    running = false;
                    break;

                case 'e':
                    System.out.println("Injecting erroneous transition ...");
                    threads[nextErroneous].sendInvalidStateTransition();
                    nextErroneous = (nextErroneous + 1) % threads.length;
                    break;

                case '+':
                    delay = Math.max(delay * 2, 1);
                    System.out.println("Delay is " + delay);
                    for (GeneratorThread t : threads) {
                        t.setDelay(delay);
                    }
                    break;

                case '-':
                    delay /= 2;
                    System.out.println("Delay is " + delay);
                    for (GeneratorThread t : threads) {
                        t.setDelay(delay);
                    }
                    break;

                default:
                    // do nothing
            }
        }

        // shutdown
        throughputLogger.shutdown();

        for (GeneratorThread t : threads) {
            t.shutdown();

            try {
                t.join();
            } catch (InterruptedException e) {
                // restore interrupted status
                Thread.currentThread().interrupt();
            }
        }
    }

    // ------------------------------------------------------------------------

    /**
     * A thread running a {@link EventsGenerator} and pushing generated events to the given
     * collector (such as Kafka / Socket / ...).
     */
    private static class GeneratorThread extends Thread {

        private final Collector<Event> out;

        private final int minAddress;
        private final int maxAddress;

        private long delay;

        private long count;

        private volatile boolean running;

        private volatile boolean injectInvalidNext;

        /**
         * Creates a new generator thread.
         *
         * @param out The collector to push the generated records to.
         * @param minAddress The lower bound for the range from which a new IP address may be
         *     picked.
         * @param maxAddress The upper bound for the range from which a new IP address may be
         *     picked.
         */
        GeneratorThread(Collector<Event> out, int minAddress, int maxAddress) {
            this.out = out;
            this.minAddress = minAddress;
            this.maxAddress = maxAddress;
            this.running = true;
        }

        @Override
        public void run() {
            final EventsGenerator generator = new EventsGenerator();

            while (running) {
                if (injectInvalidNext) {
                    injectInvalidNext = false;
                    Event next = generator.nextInvalid();
                    if (next != null) {
                        out.collect(next);
                    }
                } else {
                    out.collect(generator.next(minAddress, maxAddress));
                }

                count += 1;

                // sleep the delay to throttle
                if (delay > 0) {
                    try {
                        Thread.sleep(delay);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }

        public long currentCount() {
            return count;
        }

        public void shutdown() {
            running = false;
            interrupt();
        }

        public void setDelay(long delay) {
            this.delay = delay;
        }

        public void sendInvalidStateTransition() {
            injectInvalidNext = true;
        }
    }

    // ------------------------------------------------------------------------

    /** Thread that periodically print the number of elements generated per second. */
    private static class ThroughputLogger extends Thread {

        private final GeneratorThread[] generators;

        private volatile boolean running;

        /**
         * Instantiates the throughput logger.
         *
         * @param generators The generator threads whose aggregate throughput should be logged.
         */
        ThroughputLogger(GeneratorThread[] generators) {
            this.generators = generators;
            this.running = true;
        }

        @Override
        public void run() {
            long lastCount = 0L;
            long lastTimeStamp = System.currentTimeMillis();

            while (running) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    break;
                }

                long ts = System.currentTimeMillis();
                long currCount = 0L;
                for (GeneratorThread generator : generators) {
                    currCount += generator.currentCount();
                }

                double factor = (ts - lastTimeStamp) / 1000.0;
                double perSec = (currCount - lastCount) / factor;

                lastTimeStamp = ts;
                lastCount = currCount;

                System.out.println(perSec + " / sec");
            }
        }

        public void shutdown() {
            running = false;
            interrupt();
        }
    }
}
