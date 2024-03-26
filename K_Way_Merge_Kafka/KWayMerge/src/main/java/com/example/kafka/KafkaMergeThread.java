package com.example.kafka;

import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
public class KafkaMergeThread implements  Runnable {
        private volatile boolean running = true;
        private final int partitionCount;
        private ConcurrentLinkedQueue<kafkaMessage> partitionQueue [];
        PriorityQueue<minHeapTuple> minHeap;

        public KafkaMergeThread(int partitionCount, ConcurrentLinkedQueue<kafkaMessage>[] queue) {
            this.partitionCount = partitionCount;
            this.partitionQueue = queue;
            Comparator<minHeapTuple> tupleComparator = Comparator.comparingInt(t -> t.priority);
            this.minHeap = new PriorityQueue<>(tupleComparator);
        }
        @Override
        public void run() {
            int waitTimeEmptyQueueMilli = 100;
            boolean foundEmptyQueue = false;
            boolean queueInitialized = false;


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
                        minHeapTuple curr = new minHeapTuple(q.poll().seqNum, q);
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
                    minHeapTuple curr = new minHeapTuple(nextNumUnpacked, q);
                    minHeap.add(curr);
                }
            }
        }
        public void stopRunning() {
            running = false;
        }

    static class minHeapTuple{
        int priority;
        ConcurrentLinkedQueue<kafkaMessage> q;
        public minHeapTuple(int priority, ConcurrentLinkedQueue<kafkaMessage> queue) {
            this.priority = priority;
            this.q = queue;
            }
        }
    }
