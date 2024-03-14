package com.example.kafka;
import java.util.ArrayList;
import java.util.PriorityQueue;
import java.util.Comparator;


class Merge {
    public static ArrayList<Integer> merge(ArrayList<Integer>[] arr) {
        int K = arr.length;
        Comparator<minHeapTuple> tupleComparator = Comparator.comparingInt(t -> t.priority);

        PriorityQueue<minHeapTuple> minHeap = new PriorityQueue<>(tupleComparator);

        ArrayList<Integer> result = new ArrayList<>();

        for (ArrayList<Integer> integers : arr) {
            if (integers.size() > 0) {
                minHeapTuple curr = new minHeapTuple(integers.get(0), integers, 0) ;
                minHeap.add(curr);
            }
        }

        while (minHeap.size() > 0) {
            minHeapTuple smallest = minHeap.remove();
            result.add(smallest.priority);
            ArrayList<Integer> poppedArr = smallest.arr;
            int poppedIdx = smallest.index;
            if(poppedIdx < poppedArr.size() - 1) {
                minHeapTuple curr = new minHeapTuple(
                        poppedArr.get(poppedIdx + 1),
                        poppedArr,
                        poppedIdx + 1);
                minHeap.add(curr);
            }
        }

        return result;
    }
}

class minHeapTuple{
    int priority;
    ArrayList<Integer> arr;
    int index;
    public minHeapTuple(int priority, ArrayList<Integer> arr, int index) {
        this.priority = priority;
        this.arr = arr;
        this.index = index;
    }
}
