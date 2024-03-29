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

package org.example;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class KafkaMergeThread implements  Runnable {
        private volatile boolean running = true;
        private  static final int throughputIntervalMilli = 100;
        private final int partitionCount;
        private ConcurrentLinkedQueue<kafkaMessage> partitionQueue [];
        PriorityQueue<minHeapTuple> minHeap;
        public ArrayList<Long> latencies;
        public ArrayList<Double> throughput;

    public KafkaMergeThread(int partitionCount, ConcurrentLinkedQueue<kafkaMessage>[] queue) {
            this.partitionCount = partitionCount;
            this.partitionQueue = queue;
            Comparator<minHeapTuple> tupleComparator = Comparator.comparingInt(t -> t.priority);
            this.minHeap = new PriorityQueue<>(tupleComparator);
            this.latencies = new ArrayList<>();
            this.throughput = new ArrayList<>();
    }
        @Override
        public void run() {
            int waitTimeEmptyQueueMilli = 100;
            boolean foundEmptyQueue = false;
            boolean queueInitialized = false;
            long startTime = System.currentTimeMillis();
            int numEvents = 0;

            // Initial queue instantiation
            while(!queueInitialized) {
                for (ConcurrentLinkedQueue<kafkaMessage> q : partitionQueue) {
                    if (q.size() == 0) {
                        foundEmptyQueue = true;
                        break;
                    }
                }
                if(!foundEmptyQueue) {
                    for (ConcurrentLinkedQueue<kafkaMessage> q : partitionQueue) {
                        kafkaMessage temp = q.poll();
                        minHeapTuple curr = new minHeapTuple(temp.seqNum, q, temp.arrivalTime);
                        minHeap.add(curr);
                    }
                    queueInitialized = true;
                }
                foundEmptyQueue = false;
            }

            while (running) {
                // Pop smallest item
                minHeapTuple smallest = minHeap.remove();
                ConcurrentLinkedQueue<kafkaMessage> q = smallest.q;
                int sequenceNum = smallest.priority;
                long processingTime = System.nanoTime() - smallest.createTime;

                latencies.add(processingTime);
                numEvents++;

//                System.out.print(sequenceNum + " "); // uncomment to verify correctness

                // Keep polling that queue until there is a number in there
                kafkaMessage nextNum = q.poll();
                while(nextNum == null && running) {
                    try {
                        Thread.sleep(waitTimeEmptyQueueMilli);
                    } catch (Exception e) {
                        System.out.println("Some issue sleeping the cpu");
                    }
                    nextNum = q.poll();
                }

                // Append the new value to the queue
                if (running) {
                    int nextNumUnpacked = nextNum.seqNum;
                    minHeapTuple curr = new minHeapTuple(nextNumUnpacked, q, nextNum.arrivalTime);
                    minHeap.add(curr);
                }
                long totalTime = (System.currentTimeMillis() - startTime) / 1000 ;
                double throughput_curr = (double) numEvents / totalTime;
                this.throughput.add(throughput_curr);
            }
        }
        public void stopRunning() {
            running = false;
            System.out.println("Latency Values " + this.latencies.size());
            System.out.println("Throughput values: " + this.throughput.size());
        }

    static class minHeapTuple{
        int priority;
        ConcurrentLinkedQueue<kafkaMessage> q;
        long createTime;
        public minHeapTuple(int priority, ConcurrentLinkedQueue<kafkaMessage> queue, long time) {
            this.priority = priority;
            this.q = queue;
            this.createTime = time;
            }
        }
    }
