package eu.stratosphere.streaming.test.cellinfo;

public interface IWorkerEngine {
	public int get(long timeStamp, long lastMillis, int cellId);
	public void put(int cellId, long timeStamp);
}
