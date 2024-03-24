package com.example.kafka;

import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentLinkedQueue;

import java.util.ArrayList;
public class KafkaMergeThread implements  Runnable {
        private volatile boolean running = true;
        private final int partitionCount;
        private ConcurrentLinkedQueue<Integer> partitionQueue [];
        PriorityQueue<minHeapTuple> minHeap;

        public KafkaMergeThread(int partitionCount, ConcurrentLinkedQueue<Integer>[] queue) {
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
                for (ConcurrentLinkedQueue<Integer> q : partitionQueue) {
                    if (q.size() == 0) {
                        foundEmptyQueue = true;
                        break;
                    }
                }
                if(!foundEmptyQueue) {
                    for (ConcurrentLinkedQueue<Integer> q : partitionQueue) {
                        minHeapTuple curr = new minHeapTuple(q.poll(), q);
                        minHeap.add(curr);
                    }
                    queueInitialized = true;
                }
                foundEmptyQueue = false;
            }

            while (running) {
                // Pop smallest item
                minHeapTuple smallest = minHeap.remove();
                ConcurrentLinkedQueue<Integer> q = smallest.q;
                int sequenceNum = smallest.priority;
                System.out.print(sequenceNum + " ");

                // Keep polling that queue until there is a number in there
                Integer nextNum = q.poll();
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
                    int nextNumUnpacked = nextNum;
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
        ConcurrentLinkedQueue<Integer> q;
        public minHeapTuple(int priority, ConcurrentLinkedQueue<Integer> queue) {
            this.priority = priority;
            this.q = queue;
            }
        }
    }
