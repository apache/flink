package eu.stratosphere.pact.programs.pagerank.tasks;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.eclipse.jetty.util.BlockingArrayQueue;

import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.iterative.nephele.tasks.AbstractIterativeTask;
import eu.stratosphere.pact.iterative.nephele.util.IterationIterator;
import eu.stratosphere.pact.programs.pagerank.types.VertexNeighbourPartial;
import eu.stratosphere.pact.programs.pagerank.types.VertexNeighbourPartialAccessor;
import eu.stratosphere.pact.programs.pagerank.types.VertexPageRank;
import eu.stratosphere.pact.programs.pagerank.types.VertexPageRankAccessor;
import eu.stratosphere.pact.runtime.iterative.MutableHashTable;
import eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2;
import eu.stratosphere.pact.runtime.plugable.TypeComparator;
import eu.stratosphere.pact.runtime.util.EmptyMutableObjectIterator;

public class ParallelVertexRankMatchBuild extends AbstractIterativeTask {

  public static HashMap<Integer, MutableHashTable<Value, VertexNeighbourPartial>> tables =
      new  HashMap<Integer, MutableHashTable<Value,VertexNeighbourPartial>>();

  private static volatile List<MemorySegment> joinMem;
  private static volatile boolean firstRun  = true;
  private static final AtomicInteger countFinnished = new AtomicInteger(0);

  private static final Object MAGIC = new Object();
  private static final int MATCH_CHUNCK_SIZE = 1024*1024;

  private int iteration = 0;

  @Override
  public void runIteration(IterationIterator iterationIter) throws Exception {
    MutableHashTable<Value, VertexNeighbourPartial> table = null;
    synchronized(MAGIC) {
      if (tables.get(iteration) == null) {
        if (firstRun) {
          int chunckSize = MATCH_CHUNCK_SIZE;

          joinMem =
            memoryManager.allocateStrict(this, (int) (memorySize/chunckSize), chunckSize);
          firstRun = false;
        }

        TypeAccessorsV2 buildAccess = new VertexPageRankAccessor();
        TypeAccessorsV2 probeAccess = new VertexNeighbourPartialAccessor();
        TypeComparator comp = new MatchComparator();

        if (iteration > 0) {
          tables.get(iteration - 1).close();
        }

        table = new MutableHashTable<Value, VertexNeighbourPartial>(buildAccess, probeAccess, comp,
            joinMem, ioManager, 128, true);
        table.openParallel();
        tables.put(iteration, table);
        countFinnished.set(0);
      } else {
        table = tables.get(iteration);
      }
    }

    table.buildParallelInitialTable(iterationIter);

    int finnished = countFinnished.incrementAndGet();
    if (finnished == getEnvironment().getCurrentNumberOfSubtasks() / 4) {
      table.finnishParalle(EmptyMutableObjectIterator.<VertexNeighbourPartial>get());
    }

    iteration++;
  }

  @Override
  protected void initTask() {
  }

  @Override
  public int getNumberOfInputs() {
    return 1;
  }

  public static final class MatchComparator implements TypeComparator<VertexNeighbourPartial,
  VertexPageRank>
  {
    private long key;

    @Override
    public void setReference(VertexNeighbourPartial reference,
        TypeAccessorsV2<VertexNeighbourPartial> accessor) {
      this.key = reference.getVid();
    }

    @Override
    public boolean equalToReference(VertexPageRank candidate, TypeAccessorsV2<VertexPageRank> accessor) {
      return this.key == candidate.getVid();
    }
  }
}
