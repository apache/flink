package eu.stratosphere.pact.runtime.hash;
import java.util.Iterator;


public class RangeIterator implements Iterator<Integer> {

	private final int maxValue;
	private int currentValue;
	
	public RangeIterator(int minValue, int maxValue)
	{
		this.maxValue = maxValue;
		currentValue = minValue;
	}
	
	
	@Override
	public boolean hasNext() {
		return (currentValue < maxValue);
	}

	@Override
	public Integer next() {
		return currentValue++;
	}


	@Override
	public void remove() {
		// TODO Auto-generated method stub
	}

}
