package eu.stratosphere.pact.runtime.hash;
import java.util.Iterator;


public class StepRangeIterator implements Iterator<Integer> {

	private final int maxValue;
	private int currentValue;
	private final int step;
	
	public StepRangeIterator(int minValue, int maxValue, int step)
	{
		this.maxValue = maxValue;
		currentValue = minValue;
		this.step = step;
	}
	
	
	@Override
	public boolean hasNext() {
		return (currentValue < maxValue);
	}

	@Override
	public Integer next() {
		int temp = currentValue;
		currentValue += step;
		return temp;
	}


	@Override
	public void remove() {
		// TODO Auto-generated method stub
	}

}
