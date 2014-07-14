package eu.stratosphere.streaming.test.window.wordcount;

public class IncrementalWindow {

	private int currentTupleNum;
	private int fullTupleNum;
	private int slideTupleNum;
	
	public IncrementalWindow(int batchRange, int windowSize, int slidingStep){}
	
	void pushBack(){}
	
	void popFront(){}
}
