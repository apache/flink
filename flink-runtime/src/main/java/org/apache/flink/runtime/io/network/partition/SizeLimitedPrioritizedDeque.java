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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.io.AvailabilityProvider;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.metrics.TimerGauge;
import org.apache.flink.runtime.plugable.SerializationDelegate;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * A deque-like data structure that supports prioritization of elements, such they will be polled
 * before any non-priority elements.
 *
 * <p>{@implNote The current implementation deliberately does not implement the respective interface
 * to minimize the maintenance effort. Furthermore, it's optimized for handling non-priority
 * elements, such that all operations for adding priority elements are much slower than the
 * non-priority counter-parts.}
 *
 * <p>Note that all element tests are performed by identity.
 *
 * @param <T> the element type.
 */
@Internal
public class SizeLimitedPrioritizedDeque<T> extends PrioritizedDeque<T>
        implements AvailabilityProvider {
    // todo hx: There are two types of objects,
    // 1. For example, Event/broadcast objects have been serialized, and the memory size can be
    // determined by the serialized size
    // 2. StreamRecord, the memory size cannot be determined

    private AvailabilityHelper availabilityHelper = new AvailabilityHelper();

    private long estimateSize = 0;

    private long maxDequeMemorySize = 2 * 32 * 1024; // 32 KB

    // save sample record size
    private final LinkedList<Integer> samples = new LinkedList<>();

    private final int maxSampleLength = 100;

    private final int minSampleLength = 10;

    // todo hx: sampleLenth calculate function need to be adjust
    private int sampleLength = minSampleLength;

    private int avgElementMemorySize = 0;

    private final int sampleInterval = 50;

    private int notSampledElementCnt = 0;

    public final DataOutputSerializer serializer;

    public SizeLimitedPrioritizedDeque() {
        this.availabilityHelper.resetAvailable();
        this.serializer = new DataOutputSerializer(128);
    }

    @Override
    public CompletableFuture<?> getAvailableFuture() {
        return availabilityHelper.getAvailableFuture();
    }

    // return Object Memory Size
    public int blockingWaitingForAdd(SerializationDelegate<?> record, TimerGauge gauge)
            throws ExecutionException, InterruptedException {
        int size = avgElementMemorySize;
        if (samples.size() < minSampleLength || notSampledElementCnt >= sampleInterval) {
            notSampledElementCnt = 0;
            size = getObjectSizeInMemory(record);
            samples.add(size);

            double stdDeviation = relativeSampleStandardDeviation(samples);
            sampleLength = Math.max((int) stdDeviation * maxSampleLength, minSampleLength);
            while (samples.size() > sampleLength) {
                samples.removeFirst();
            }
        } else {
            notSampledElementCnt++;
        }
        if (estimateSize < maxDequeMemorySize) {
            return size;
        }
        gauge.markStart();
        while (estimateSize >= maxDequeMemorySize) {
            this.availabilityHelper.resetUnavailable();
            this.getAvailableFuture().get();
        }
        gauge.markEnd();
        return size;
    }

    // should be guarded in PipelinedSubpartition.buffers
    public int addAndGetSize(T element) {
        //        assert Thread.holdsLock()
        super.add(element);
        avgElementMemorySize = samples.isEmpty() ? 1 : (int) average(samples);
        estimateSize = avgElementMemorySize * super.size();
        if (estimateSize >= maxDequeMemorySize) {
            this.availabilityHelper.resetUnavailable();
        }
        return avgElementMemorySize;
    }

    public int getObjectSizeInMemory(SerializationDelegate<?> record) {
        final ByteBuffer serializedBuffer;
        //                serializedBuffer = EventSerializer.toSerializedEvent((AbstractEvent)
        // element);
        try {
            serializedBuffer = RecordWriter.serializeRecord(serializer, record);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return serializedBuffer.remaining();
    }

    // should be guarded in PipelinedSubpartition.buffers
    public Tuple2<T, Integer> pollElementAndSize() {
        T polled = super.poll();
        if (polled == null) {
            return null;
        }

        estimateSize = avgElementMemorySize * super.size();

        CompletableFuture<?> toNotify = null;
        if (!availabilityHelper.isApproximatelyAvailable() && estimateSize < maxDequeMemorySize) {
            toNotify = availabilityHelper.getUnavailableToResetAvailable();
        }
        if (toNotify != null) {
            //            System.out.printf("notify now\n");
            toNotify.complete(null);
        }
        return new Tuple2<>(polled, avgElementMemorySize);
    }

    public static double relativeSampleStandardDeviation(List<Integer> datas) {
        double sum = 0, mean = 0, sampleStdVariance = 0;
        for (Integer data : datas) {
            sum += data;
        }
        mean = sum / datas.size();

        for (Integer data : datas) {
            sampleStdVariance = sampleStdVariance + (data - mean) * (data - mean);
        }
        sampleStdVariance = Math.sqrt(sampleStdVariance / (datas.size() - 1));

        return sampleStdVariance / mean;
    }

    public static double average(List<Integer> datas) {
        double sum = 0;
        for (Integer data : datas) {
            sum += data;
        }
        return sum / datas.size();
    }
}
