package eu.stratosphere.pact.programs.connected.tasks;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.iterative.nephele.tasks.IterationHead;
import eu.stratosphere.pact.programs.connected.types.ComponentUpdate;
import eu.stratosphere.pact.programs.connected.types.ComponentUpdateAccessor;
import eu.stratosphere.pact.programs.connected.types.LazyTransitiveClosureEntry;
import eu.stratosphere.pact.programs.connected.types.TransitiveClosureEntry;
import eu.stratosphere.pact.programs.connected.types.TransitiveClosureEntryAccessors;
import eu.stratosphere.pact.runtime.iterative.MutableHashTable;
import eu.stratosphere.pact.runtime.iterative.MutableHashTable.LazyHashBucketIterator;
import eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2;
import eu.stratosphere.pact.runtime.plugable.TypeComparator;
import eu.stratosphere.pact.runtime.util.EmptyMutableObjectIterator;

public class UpdateableMatching extends IterationHead {

  protected static final Log LOG = LogFactory.getLog(UpdateableMatching.class);

  public static final int MATCH_CHUNCK_SIZE = 1024 * 1024;
  private MutableHashTable<Value, ComponentUpdate> table;

  @Override
  public void finish(MutableObjectIterator<Value> iter,
      OutputCollectorV2 output) throws Exception {
    //HashBucketIterator<PactRecord, PactRecord> stateIter = table.getBuildSideIterator();

    //PactRecord record = new PactRecord();
    //PactRecord result = new PactRecord();
  }

  @Override
  public void processInput(MutableObjectIterator<Value> iter,
      OutputCollectorV2 output) throws Exception {
    // Load build side into table
    int chunckSize = MATCH_CHUNCK_SIZE;
    List<MemorySegment> joinMem = memoryManager.allocateStrict(this, (int) (memorySize/chunckSize), chunckSize);

    TypeAccessorsV2 buildAccess = new TransitiveClosureEntryAccessors();
    TypeAccessorsV2 probeAccess = new ComponentUpdateAccessor();
    TypeComparator comp = new MatchComparator();

    table = new MutableHashTable<Value, ComponentUpdate>(buildAccess, probeAccess, comp,
        joinMem, ioManager, 128);
    table.open(inputs[1], EmptyMutableObjectIterator.<ComponentUpdate>get());

    // Process input as normally
    processUpdates(iter, output);
  }

  @Override
  public void processUpdates(MutableObjectIterator<Value> iter,
      OutputCollectorV2 output) throws Exception {
    LazyTransitiveClosureEntry state = new LazyTransitiveClosureEntry();
    ComponentUpdate probe = new ComponentUpdate();

    int countUpdated = 0;
    int countUnchanged = 0;

    while (iter.next(probe)) {
      LazyHashBucketIterator<Value, ComponentUpdate> tableIter = table.getLazyMatchesFor(probe);
      if (tableIter.next(state)) {
        long oldCid = state.getCid();
        long updateCid = probe.getCid();

        if (updateCid < oldCid) {
          state.setCid(updateCid);
          //tableIter.writeBack(state);
          output.collect(state);
          countUpdated++;
        } else {
          countUnchanged++;
        }
      }
      if (tableIter.next(state)) {
        throw new RuntimeException("there should only be one");
      }
    }

    LOG.info("Processing stats - Updated: " + countUpdated + " - Unchanged:" + countUnchanged);
  }

  @Override
  public int getNumberOfInputs() {
    return 2;
  }

  private static final class MatchComparator implements TypeComparator<ComponentUpdate, TransitiveClosureEntry>
  {
    private long key;

    @Override
    public void setReference(ComponentUpdate reference,
        TypeAccessorsV2<ComponentUpdate> accessor) {
      this.key = reference.getVid();
    }

    @Override
    public boolean equalToReference(TransitiveClosureEntry candidate, TypeAccessorsV2<TransitiveClosureEntry> accessor) {
      return this.key == candidate.getVid();
    }
  }
}
