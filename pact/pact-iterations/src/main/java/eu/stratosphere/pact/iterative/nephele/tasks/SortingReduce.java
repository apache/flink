package eu.stratosphere.pact.iterative.nephele.tasks;

import java.util.Comparator;
import java.util.Iterator;

import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.iterative.nephele.util.IterationIterator;
import eu.stratosphere.pact.runtime.sort.UnilateralSortMerger;
import eu.stratosphere.pact.runtime.task.util.OutputCollector;
import eu.stratosphere.pact.runtime.util.KeyComparator;
import eu.stratosphere.pact.runtime.util.KeyGroupedIterator;

public abstract class SortingReduce extends AbstractIterativeTask {

  private int[] keyPos;
  private Class<? extends Key>[] keyClasses;
  private Comparator<Key>[] comparators;

  private UnilateralSortMerger sorter;

  @Override
  public void runIteration(IterationIterator iterationIter) throws Exception {
    PactRecordIterator pactIter = getInputIterator(iterationIter);

    try {
      //System.out.println(memorySize);
      sorter = new UnilateralSortMerger(memoryManager, ioManager, memorySize, 64, comparators, keyPos, keyClasses,
         pactIter, this, 0.8f);
    } catch (Exception ex) {
      throw new RuntimeException("Error creating sorter", ex);
    }

    KeyGroupedIterator iter = new KeyGroupedIterator(sorter.getIterator(), keyPos, keyClasses);

    // run stub implementation
    while (iter.nextKey()) {
      reduce(iter.getValues(), output);
    }

    //TODO finally?
    sorter.close();
  }

  public abstract void reduce(Iterator<PactRecord> records, OutputCollector out) throws Exception;

  public abstract PactRecordIterator getInputIterator(IterationIterator iterationIter);

  public abstract int[] getKeyPos();

  public abstract Class<? extends Key>[] getKeyClasses();

  @Override
  public int getNumberOfInputs() {
    return 1;
  }

  @Override
  public void prepare() throws Exception {
    keyPos = getKeyPos();
    keyClasses = getKeyClasses();

    // create the comparators
    comparators = new Comparator[keyPos.length];
    final KeyComparator kk = new KeyComparator();
    for (int i = 0; i < comparators.length; i++) {
      comparators[i] = kk;
    }
  }
}
