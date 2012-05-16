package eu.stratosphere.pact.programs.pagerank.tasks;

import java.util.Comparator;

import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.programs.pagerank.types.VertexNeighbourPartial;
import eu.stratosphere.pact.runtime.sort.UnilateralSortMerger;
import eu.stratosphere.pact.runtime.util.KeyComparator;

public class SortPairByNeighbour extends AbstractMinimalTask {

  private int[] keyPos;
  private Class<? extends Key>[] keyClasses;
  private Comparator<Key>[] comparators;

  private UnilateralSortMerger sorter;

  @SuppressWarnings("unchecked")
  @Override
  protected void initTask() {
    keyPos = new int[] {1};
    keyClasses =  new Class[] { PactLong.class };

    // create the comparators
    comparators = new Comparator[keyPos.length];
    final KeyComparator kk = new KeyComparator();
    for (int i = 0; i < comparators.length; i++) {
      comparators[i] = kk;
    }
  }

  @Override
  public int getNumberOfInputs() {
    return 1;
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Override
  public void run() throws Exception {
    MutableObjectIterator typehidingIter = inputs[0];

    VertexNeighbourPartial result = new VertexNeighbourPartial();
    PactLong number = new PactLong();
    PactDouble con = new PactDouble();
    try {
      sorter = new UnilateralSortMerger(memoryManager, ioManager, memorySize, 64, comparators,
          keyPos, keyClasses, typehidingIter, this, 0.8f);
    } catch (Exception ex) {
      throw new RuntimeException("Error creating sorter", ex);
    }

    MutableObjectIterator<PactRecord> iter = sorter.getIterator();

    PactRecord rec = new PactRecord();
    while (iter.next(rec)) {
      long vid = rec.getField(0, number).getValue();
      long nid = rec.getField(1, number).getValue();
      double contrib = rec.getField(2, con).getValue();

      result.setVid(vid);
      result.setNid(nid);
      result.setPartial(contrib);
      output.collect(result);
    }
  }

}
