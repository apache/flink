package eu.stratosphere.pact.programs.pagerank.tasks;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.iterative.nephele.tasks.IterationHead;
import eu.stratosphere.pact.programs.pagerank.types.VertexNeighbourPartial;
import eu.stratosphere.pact.programs.pagerank.types.VertexNeighbourPartialAccessor;
import eu.stratosphere.pact.programs.pagerank.types.VertexPageRank;
import eu.stratosphere.pact.programs.pagerank.types.VertexPageRankAccessor;
import eu.stratosphere.pact.runtime.iterative.MutableHashTable;
import eu.stratosphere.pact.runtime.iterative.MutableHashTable.HashBucketIterator;
import eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2;
import eu.stratosphere.pact.runtime.plugable.TypeComparator;
import eu.stratosphere.pact.runtime.util.EmptyMutableObjectIterator;

public class VertexRankMatchingBuildCaching extends IterationHead {

  protected static final Log LOG = LogFactory.getLog(VertexRankMatchingBuildCaching.class);

  private static final int MATCH_CHUNCK_SIZE = 1024*1024;

  private VertexPageRank vRank = new VertexPageRank();

  private long matchMemory;
  private List<MemorySegment> joinMem;
  private MutableHashTable<Value, VertexPageRank> table;

  @Override
  protected void initTask() {
    super.initTask();

    accessors[1] = new VertexNeighbourPartialAccessor();
    outputAccessors[numInternalOutputs] = new VertexPageRankAccessor();

    matchMemory = memorySize;
  }

  @Override
  public void processInput(MutableObjectIterator<Value> iter,
      OutputCollectorV2 output) throws Exception {
    long chunckSize = MATCH_CHUNCK_SIZE;
    joinMem =
        memoryManager.allocateStrict(this, (int) (matchMemory/chunckSize), (int)chunckSize);

    System.out.println("Match");
    System.out.println("Mem"+matchMemory);
    System.out.println("XYZ"+joinMem.size());
    TypeAccessorsV2 buildAccess = new VertexNeighbourPartialAccessor();
    TypeAccessorsV2 probeAccess = new VertexPageRankAccessor();
    TypeComparator comp = new MatchComparator();

    table = new MutableHashTable<Value, VertexPageRank>(buildAccess, probeAccess, comp,
        joinMem, ioManager, 128);
    table.open(inputs[1], EmptyMutableObjectIterator.<VertexPageRank>get());

    processUpdates(iter, output);
  }

  @Override
  public void processUpdates(MutableObjectIterator<Value> iter,
      OutputCollectorV2 output) throws Exception {

    VertexNeighbourPartial state = new VertexNeighbourPartial();
    VertexPageRank pageRank = new VertexPageRank();

    VertexPageRank result = new VertexPageRank();

    while (iter.next(pageRank)) {
      HashBucketIterator<Value, VertexPageRank> tableIter = table.getMatchesFor(pageRank);
      while (tableIter.next(state)) {
        double rank = pageRank.getRank();
        double partial = state.getPartial();

        if (Double.isNaN(rank*partial)) {
          LOG.info("NAN: "  + pageRank.getVid() + "::" + rank + " // " + pageRank.getRank() +"::"+ state.getPartial() );
        } else {
          result.setVid(state.getNid());
          result.setRank(rank*partial);
          output.collect(result);
        }
      }
    }
  }

  @Override
  public int getNumberOfInputs() {
    return 2;
  }

  @Override
  public void finish(MutableObjectIterator<Value> iter,
      OutputCollectorV2 output) throws Exception {
    //forwardRecords(iter, output);
    //table.close();
  }

  private final void forwardRecords(MutableObjectIterator<Value> iter,
      OutputCollectorV2 output) throws Exception {
    while (iter.next(vRank)) {
      output.collect(vRank);
    }
  }

  private static final class MatchComparator implements TypeComparator<VertexPageRank,
    VertexNeighbourPartial>
  {
    private long key;

    @Override
    public void setReference(VertexPageRank reference,
        TypeAccessorsV2<VertexPageRank> accessor) {
      this.key = reference.getVid();
    }

    @Override
    public boolean equalToReference(VertexNeighbourPartial candidate, TypeAccessorsV2<VertexNeighbourPartial> accessor) {
      return this.key == candidate.getVid();
    }
  }
}
