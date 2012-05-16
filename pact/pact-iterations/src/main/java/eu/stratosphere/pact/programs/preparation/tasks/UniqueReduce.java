package eu.stratosphere.pact.programs.preparation.tasks;

import java.util.Comparator;

import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.iterative.nephele.tasks.AbstractMinimalTask;
import eu.stratosphere.pact.runtime.sort.UnilateralSortMerger;
import eu.stratosphere.pact.runtime.util.KeyComparator;
import eu.stratosphere.pact.runtime.util.KeyGroupedIterator;
import eu.stratosphere.pact.runtime.util.KeyGroupedIterator.ValuesIterator;

public class UniqueReduce extends AbstractMinimalTask {

  private int[] keyPos;
  private Class<? extends Key>[] keyClasses;
  private Comparator<Key>[] comparators;

  private UnilateralSortMerger sorter;

  @SuppressWarnings("unchecked")
  @Override
  protected void initTask() {
    keyPos = config.getLocalStrategyKeyPositions(0);
    keyClasses =  loadKeyClasses();

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

  @Override
  public void run() throws Exception {
    RecordWriter<Value> listWriter = output.getWriters().get(0);
    MutableObjectIterator typelessIter = inputs[0];

    try {
      sorter = new UnilateralSortMerger(memoryManager, ioManager, memorySize, 128, comparators,
          keyPos, keyClasses, typelessIter, this, 0.8f);
    } catch (Exception ex) {
      System.out.println(ex);
      System.out.flush();
      throw new Exception("Error creating sorter", ex);
    }

    KeyGroupedIterator iter = new KeyGroupedIterator(sorter.getIterator(), keyPos, keyClasses);

    while (iter.nextKey()) {
      //Only send one record for records with the same key
      ValuesIterator valueIter = iter.getValues();
      PactRecord value = null;
      while (valueIter.hasNext()) {
        value = valueIter.next();
      }

      listWriter.emit(value);
    }
  }

}
