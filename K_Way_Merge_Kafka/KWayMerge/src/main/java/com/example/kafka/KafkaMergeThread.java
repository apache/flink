package com.example.kafka;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class KafkaMergeThread implements  Runnable {
        private volatile boolean running = true;
        private  static final int throughputIntervalMilli = 200;
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
                    startTime = System.currentTimeMillis();
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
                long currentTime = System.currentTimeMillis();

                if (currentTime - startTime > throughputIntervalMilli) {
                    double totalTimeInSeconds = (currentTime - startTime) / 1000.0;
                    double curr_throughput = numEvents / totalTimeInSeconds;
                    throughput.add(curr_throughput);
                    numEvents = 0;
                    startTime = currentTime;
                }

                System.out.print(sequenceNum + " ");

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
            }
        }
        public void stopRunning() {
            running = false;
            System.out.println("Latency's " + this.latencies.size());
            System.out.println("Latency's " + this.throughput.size());
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
