package eu.stratosphere.nephele.streaming.profiling;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;

public class ProfilingValueStatistic {

	private ArrayList<ProfilingValue> sortedByValue;

	private LinkedList<ProfilingValue> sortedById;

	private int statisticWindowSize;

	private int noOfStoredValues;

	private double sumOfValues;

	public ProfilingValueStatistic(int statisticWindowSize) {
		this.sortedById = new LinkedList<ProfilingValue>();
		this.sortedByValue = new ArrayList<ProfilingValue>();
		this.statisticWindowSize = statisticWindowSize;
		this.noOfStoredValues = 0;
		this.sumOfValues = 0;
	}
	
	public void addValue(ProfilingValue value) {
		ProfilingValue droppedValue = insertIntoSortedByTimestamp(value);

		if (droppedValue != null) {
			removeFromSortedByValue(droppedValue);
			noOfStoredValues--;
			sumOfValues -= droppedValue.getValue();
		}

		insertIntoSortedByValue(value);
		noOfStoredValues++;
		sumOfValues += value.getValue();
	}

	private ProfilingValue insertIntoSortedByTimestamp(ProfilingValue value) {
		if (!sortedById.isEmpty() && sortedById.getLast().getId() >= value.getId()) {
			throw new IllegalArgumentException("Trying to add stale profiling values. This should not happen.");
		}
		sortedById.add(value);

		if (noOfStoredValues >= statisticWindowSize) {
			return sortedById.removeFirst();
		} else {
			return null;
		}
	}

	protected void insertIntoSortedByValue(ProfilingValue value) {
		int insertionIndex = Collections.binarySearch(sortedByValue, value);
		if (insertionIndex < 0) {
			insertionIndex = -(insertionIndex + 1);
		}
		
		sortedByValue.add(insertionIndex, value);
	}

	protected void removeFromSortedByValue(ProfilingValue toRemove) {
		int removeIndex = Collections.binarySearch(sortedByValue, toRemove);
		if (removeIndex < 0) {
			throw new IllegalArgumentException("Trying to drop inexistant profiling value. This should not happen.");
		}
		sortedByValue.remove(removeIndex);
	}

	public double getMedianValue() {
		if (noOfStoredValues == 0) {
			throw new RuntimeException("Cannot calculate median of empty value set");
		}

		int medianIndex = noOfStoredValues / 2;
		return sortedByValue.get(medianIndex).getValue();
	}

	public double getMaxValue() {
		if (noOfStoredValues == 0) {
			throw new RuntimeException("Cannot calculate the max value of empty value set");
		}
		return sortedByValue.get(noOfStoredValues - 1).getValue();
	}

	public double getMinValue() {
		if (noOfStoredValues == 0) {
			throw new RuntimeException("Cannot calculate the min value of empty value set");
		}
		return sortedByValue.get(0).getValue();
	}

	public double getArithmeticMean() {
		if (noOfStoredValues == 0) {
			throw new RuntimeException("Cannot calculate the arithmetic mean of empty value set");
		}

		return sumOfValues / noOfStoredValues;
	}
	
	public boolean hasValues() {
		return noOfStoredValues > 0;
	}
}
