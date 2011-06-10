package eu.stratosphere.nephele.template;


/**
 * 
 */
public interface InputSplitAssigner<T extends InputSplit>
{

	boolean assignInputSplits(T[] splits);
}
