package eu.stratosphere.streaming.cellinfo;

import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

public class WorkerEngineExact implements java.io.Serializable, IWorkerEngine {
  private long lastTimeUpdated_;
  private long bufferInterval_;
  private TreeMap<Long, Integer>[] counters_;

  @SuppressWarnings("unchecked")
  public WorkerEngineExact(int numOfCells, int bufferInterval, long currentTime) {
    lastTimeUpdated_ = currentTime;
    bufferInterval_ = bufferInterval;
    counters_ = new TreeMap[numOfCells];
    for (int i = 0; i < numOfCells; ++i) {
      counters_[i] = new TreeMap<Long, Integer>();
    }
  }

  public int get(long timeStamp, long lastMillis, int cellId) {
    refresh(timeStamp);
    Map<Long, Integer> subMap = counters_[cellId].subMap(timeStamp -
      lastMillis, true, timeStamp, false);
    int retVal = 0;
    for (Map.Entry<Long, Integer> entry : subMap.entrySet()) {
      retVal += entry.getValue();
    }
    return retVal;
  }

  public void put(int cellId, long timeStamp) {
    refresh(timeStamp);
    TreeMap<Long, Integer> map = counters_[cellId];
    // System.out.println(map.size());
    if (map.containsKey(timeStamp)) {
      map.put(timeStamp, map.get(timeStamp) + 1);
    } else {
      map.put(timeStamp, 1);
    }
  }

  public void refresh(long timeStamp) {
    if (timeStamp - lastTimeUpdated_ > bufferInterval_) {
      // System.out.println("Refresh at " + timeStamp);
      for (int i = 0; i < counters_.length; ++i) {
        for (Iterator<Map.Entry<Long, Integer>> it = counters_[i].entrySet()
          .iterator(); it.hasNext();) {
          Map.Entry<Long, Integer> entry = it.next();
          long time = entry.getKey();
          if (timeStamp - time > bufferInterval_) {
            // System.out.println("Remove: " + i + "_" + time + " at " +
            // timeStamp);
            it.remove();
          } else {
            break;
          }
        }
      }
      lastTimeUpdated_ = timeStamp;
    }
  }
}
