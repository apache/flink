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

package org.apache.flink.runtime.taskmanager;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;

import org.slf4j.Logger;

import javax.management.MBeanServer;

import java.lang.management.BufferPoolMXBean;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.management.MemoryUsage;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * A thread the periodically logs statistics about:
 *
 * <ul>
 *   <li>Heap and non-heap memory usage
 *   <li>Memory pools and pool usage
 *   <li>Garbage collection times and counts
 * </ul>
 */
public class MemoryLogger extends Thread {

    private final Logger logger;

    private final long interval;

    private final MemoryMXBean memoryBean;

    private final List<MemoryPoolMXBean> poolBeans;

    private final List<GarbageCollectorMXBean> gcBeans;

    private final BufferPoolMXBean directBufferBean;

    private final CompletableFuture<Void> monitored;

    private volatile boolean running = true;

    public static void startIfConfigured(
            Logger logger,
            Configuration configuration,
            CompletableFuture<Void> taskManagerTerminationFuture) {
        if (!logger.isInfoEnabled()
                || !configuration.getBoolean(TaskManagerOptions.DEBUG_MEMORY_LOG)) {
            return;
        }
        logger.info("Starting periodic memory usage logger");

        new MemoryLogger(
                        logger,
                        configuration.getLong(
                                TaskManagerOptions.DEBUG_MEMORY_USAGE_LOG_INTERVAL_MS),
                        taskManagerTerminationFuture)
                .start();
    }

    /**
     * Creates a new memory logger that logs in the given interval and lives until the given
     * termination future completes.
     *
     * @param logger The logger to use for outputting the memory statistics.
     * @param interval The interval in which the thread logs.
     * @param monitored termination future for the system to whose life the thread is bound. The
     *     thread terminates once the system terminates.
     */
    public MemoryLogger(Logger logger, long interval, CompletableFuture<Void> monitored) {
        super("Memory Logger");
        setDaemon(true);
        setPriority(Thread.MIN_PRIORITY);

        this.logger = logger;
        this.interval = interval;
        this.monitored = monitored;

        this.memoryBean = ManagementFactory.getMemoryMXBean();
        this.poolBeans = ManagementFactory.getMemoryPoolMXBeans();
        this.gcBeans = ManagementFactory.getGarbageCollectorMXBeans();

        // The direct buffer pool bean needs to be accessed via the bean server
        MBeanServer beanServer = ManagementFactory.getPlatformMBeanServer();
        BufferPoolMXBean directBufferBean = null;
        try {
            directBufferBean =
                    ManagementFactory.newPlatformMXBeanProxy(
                            beanServer,
                            "java.nio:type=BufferPool,name=direct",
                            BufferPoolMXBean.class);
        } catch (Exception e) {
            logger.warn("Failed to initialize direct buffer pool bean.", e);
        } finally {
            this.directBufferBean = directBufferBean;
        }
    }

    public void shutdown() {
        this.running = false;
        interrupt();
    }

    // ------------------------------------------------------------------------

    @Override
    public void run() {
        try {
            while (running && (monitored == null || !monitored.isDone())) {
                logger.info(getMemoryUsageStatsAsString(memoryBean));
                logger.info(getDirectMemoryStatsAsString(directBufferBean));
                logger.info(getMemoryPoolStatsAsString(poolBeans));
                logger.info(getGarbageCollectorStatsAsString(gcBeans));

                try {
                    Thread.sleep(interval);
                } catch (InterruptedException e) {
                    if (running) {
                        throw e;
                    }
                }
            }
        } catch (Throwable t) {
            logger.error("Memory logger terminated with exception", t);
        }
    }

    // ------------------------------------------------------------------------

    /**
     * Gets the memory footprint of the JVM in a string representation.
     *
     * @return A string describing how much heap memory and direct memory are allocated and used.
     */
    public static String getMemoryUsageStatsAsString(MemoryMXBean memoryMXBean) {
        MemoryUsage heap = memoryMXBean.getHeapMemoryUsage();
        MemoryUsage nonHeap = memoryMXBean.getNonHeapMemoryUsage();

        long heapUsed = heap.getUsed() >> 20;
        long heapCommitted = heap.getCommitted() >> 20;
        long heapMax = heap.getMax() >> 20;

        long nonHeapUsed = nonHeap.getUsed() >> 20;
        long nonHeapCommitted = nonHeap.getCommitted() >> 20;
        long nonHeapMax = nonHeap.getMax() >> 20;

        return String.format(
                "Memory usage stats: [HEAP: %d/%d/%d MB, "
                        + "NON HEAP: %d/%d/%d MB (used/committed/max)]",
                heapUsed, heapCommitted, heapMax, nonHeapUsed, nonHeapCommitted, nonHeapMax);
    }

    /**
     * Returns a String with the <strong>direct</strong> memory footprint.
     *
     * <p>These stats are not part of the other memory beans.
     *
     * @param bufferPoolMxBean The direct buffer pool bean or <code>null</code> if none available.
     * @return A string with the count, total capacity, and used direct memory.
     */
    public static String getDirectMemoryStatsAsString(BufferPoolMXBean bufferPoolMxBean) {
        if (bufferPoolMxBean == null) {
            return "Direct memory stats: unavailable";
        } else {
            return String.format(
                    "Direct memory stats: Count: %d, Total Capacity: %d, Used Memory: %d",
                    bufferPoolMxBean.getCount(),
                    bufferPoolMxBean.getTotalCapacity(),
                    bufferPoolMxBean.getMemoryUsed());
        }
    }

    /**
     * Gets the memory pool statistics from the JVM.
     *
     * @param poolBeans The collection of memory pool beans.
     * @return A string denoting the names and sizes of the memory pools.
     */
    public static String getMemoryPoolStatsAsString(List<MemoryPoolMXBean> poolBeans) {
        StringBuilder bld = new StringBuilder("Off-heap pool stats: ");
        int count = 0;

        for (MemoryPoolMXBean bean : poolBeans) {
            if (bean.getType() == MemoryType.NON_HEAP) {
                if (count > 0) {
                    bld.append(", ");
                }
                count++;

                MemoryUsage usage = bean.getUsage();
                long used = usage.getUsed() >> 20;
                long committed = usage.getCommitted() >> 20;
                long max = usage.getMax() >> 20;

                bld.append('[').append(bean.getName()).append(": ");
                bld.append(used).append('/').append(committed).append('/').append(max);
                bld.append(" MB (used/committed/max)]");
            }
        }

        return bld.toString();
    }

    /**
     * Gets the garbage collection statistics from the JVM.
     *
     * @param gcMXBeans The collection of garbage collector beans.
     * @return A string denoting the number of times and total elapsed time in garbage collection.
     */
    public static String getGarbageCollectorStatsAsString(List<GarbageCollectorMXBean> gcMXBeans) {
        StringBuilder bld = new StringBuilder("Garbage collector stats: ");

        for (GarbageCollectorMXBean bean : gcMXBeans) {
            bld.append('[')
                    .append(bean.getName())
                    .append(", GC TIME (ms): ")
                    .append(bean.getCollectionTime());
            bld.append(", GC COUNT: ").append(bean.getCollectionCount()).append(']');

            bld.append(", ");
        }

        if (!gcMXBeans.isEmpty()) {
            bld.setLength(bld.length() - 2);
        }

        return bld.toString();
    }
}
