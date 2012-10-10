package eu.stratosphere.pact.runtime.iterative.task;

import com.google.common.base.Preconditions;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.pact.common.generic.GenericMatcher;
import eu.stratosphere.pact.common.generic.types.TypeComparator;
import eu.stratosphere.pact.common.generic.types.TypeComparatorFactory;
import eu.stratosphere.pact.common.generic.types.TypePairComparatorFactory;
import eu.stratosphere.pact.common.generic.types.TypeSerializer;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.util.InstantiationUtil;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.runtime.hash.MutableHashTable;
import eu.stratosphere.pact.runtime.plugable.PactRecordComparatorFactory;
import eu.stratosphere.pact.runtime.plugable.PactRecordPairComparatorFactory;
import eu.stratosphere.pact.runtime.resettable.SpillingResettableMutableObjectIterator;
import eu.stratosphere.pact.runtime.sort.Sorter;
import eu.stratosphere.pact.runtime.sort.UnilateralSortMerger;
import eu.stratosphere.pact.runtime.task.PactDriver;
import eu.stratosphere.pact.runtime.task.PactTaskContext;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;
import eu.stratosphere.pact.runtime.util.EmptyMutableObjectIterator;
import eu.stratosphere.pact.runtime.util.ResettableMutableObjectIterator;

import java.util.List;

public class RepeatableHashJoinMatchDriver2<IT1, IT2, OT> implements PactDriver<GenericMatcher<IT1, IT2, OT>, OT> {

  protected PactTaskContext<GenericMatcher<IT1, IT2, OT>, OT> taskContext;

  private boolean firstIteration = true;

  //TODO typesafety
  private volatile MutableHashTable hashJoin;

  private ResettableMutableObjectIterator<IT1> cachedProbeSide;

  private volatile boolean running;

  @Override
  public void setup(PactTaskContext<GenericMatcher<IT1, IT2, OT>, OT> context) {
    taskContext = context;
    running = true;
  }

  @Override
  public int getNumberOfInputs() {
    return 2;
  }

  @Override
  public Class<GenericMatcher<IT1, IT2, OT>> getStubType() {
    @SuppressWarnings("unchecked")
    final Class<GenericMatcher<IT1, IT2, OT>> clazz =
        (Class<GenericMatcher<IT1, IT2, OT>>) (Class<?>) GenericMatcher.class;
    return clazz;
  }

  @Override
  public boolean requiresComparatorOnInput() {
    return true;
  }

  @Override
  public void prepare() throws Exception {

    if (!firstIteration) {
      return;
    }

    TaskConfig config = taskContext.getTaskConfig();

    TypeSerializer<IT1> probesideSerializer = taskContext.getInputSerializer(0);
    TypeSerializer<IT2> buildsideSerializer = taskContext.getInputSerializer(1);
    TypeComparator<IT1> probesideComparator = taskContext.getInputComparator(0);
    TypeComparator<IT2> buildSideComparator = taskContext.getInputComparator(1);

    //TODO duplicated from MatchDriver, refactor
    final TypePairComparatorFactory<IT1, IT2> pairComparatorFactory;
    try {
      final Class<? extends TypePairComparatorFactory<IT1, IT2>> factoryClass =
          config.getPairComparatorFactory(taskContext.getUserCodeClassLoader());

      if (factoryClass == null) {
        @SuppressWarnings("unchecked")
        TypePairComparatorFactory<IT1, IT2> pactRecordFactory =
            (TypePairComparatorFactory<IT1, IT2>) PactRecordPairComparatorFactory.get();
        pairComparatorFactory = pactRecordFactory;
      } else {
        @SuppressWarnings("unchecked")
        final Class<TypePairComparatorFactory<IT1, IT2>> clazz = (Class<TypePairComparatorFactory<IT1, IT2>>)
            (Class<?>) TypePairComparatorFactory.class;
        pairComparatorFactory = InstantiationUtil.instantiate(factoryClass, clazz);
      }
    } catch (ClassNotFoundException e) {
      throw new Exception("The class registered as TypePairComparatorFactory cloud not be loaded.", e);
    }


    long completeMemorySize = config.getMemorySize();
    long hashJoinMemorySize = (long) (completeMemorySize * 0.25);
    config.setMemorySize(completeMemorySize - hashJoinMemorySize);

    List<MemorySegment> memSegments = taskContext.getMemoryManager().allocatePages(taskContext.getOwningNepheleTask(),
        config.getMemorySize());

    hashJoin = new MutableHashTable(buildsideSerializer, probesideSerializer, buildSideComparator,
        probesideComparator, pairComparatorFactory.createComparator12(probesideComparator, buildSideComparator),
        memSegments, taskContext.getIOManager());
  }

  //TODO refactor up into RegularPactTask
  private <T> TypeComparator<T> instantiateTypeComparator(
      Class<? extends TypeComparatorFactory<?>> comparatorFactoryClass, String keyPrefix, Configuration config,
      ClassLoader userCodeClassLoader) throws ClassNotFoundException {
    final TypeComparatorFactory<?> comparatorFactory;
    if (comparatorFactoryClass == null) {
      // fall back to PactRecord
      comparatorFactory = PactRecordComparatorFactory.get();
    } else {
      comparatorFactory = InstantiationUtil.instantiate(comparatorFactoryClass, TypeComparatorFactory.class);
    }
    return (TypeComparator<T>) comparatorFactory.createComparator(
        new TaskConfig.DelegatingConfiguration(config, keyPrefix), userCodeClassLoader);
  }

  @Override
  public void run() throws Exception {
    //TODO configure build and probeside index
    final GenericMatcher<IT1, IT2, OT> matchStub = taskContext.getStub();
    //TODO type safety
    final Collector<OT> collector = taskContext.getOutputCollector();
    final MutableObjectIterator<IT2> buildSide = taskContext.getInput(1);

    final MutableHashTable<IT2, IT1> hashJoin = Preconditions.checkNotNull(this.hashJoin);

    hashJoin.open(buildSide, EmptyMutableObjectIterator.<IT1>get());

    if (firstIteration) {
      TaskConfig config = taskContext.getTaskConfig();

      TypeSerializer<IT1> probeSideSerializer = taskContext.getInputSerializer(0);
      MemoryManager memoryManager = taskContext.getMemoryManager();
      IOManager ioManager = taskContext.getIOManager();
      AbstractInvokable owningNepheleTask = taskContext.getOwningNepheleTask();
      ClassLoader userCodeClassLoader = taskContext.getUserCodeClassLoader();

      long completeMemorySize = config.getMemorySize();
      long sorterMemorySize = (long) (completeMemorySize * 0.25);
      config.setMemorySize(completeMemorySize - sorterMemorySize);

      TypeComparator<IT1> probesideComparator = instantiateTypeComparator(
          config.getCachedHashJoinProbeSideComparatorFactoryClass(userCodeClassLoader),
          config.getCachedHashjoinProbesideComparatorPrefix(), taskContext.getTaskConfig().getConfiguration(),
          userCodeClassLoader);

      //TODO typesafety
      System.out.println(">>>>>>>>>>>>>>>>>> Starting to sort in " + owningNepheleTask.getIndexInSubtaskGroup());
      Sorter<IT1> sorter = new UnilateralSortMerger(memoryManager, ioManager, taskContext.getInput(0),
          owningNepheleTask, probeSideSerializer, probesideComparator, sorterMemorySize,
          config.getNumFilehandles(), config.getSortSpillingTreshold());

      cachedProbeSide = new SpillingResettableMutableObjectIterator<IT1>(sorter.getIterator(), probeSideSerializer,
          memoryManager, ioManager, config.getMemorySize(), owningNepheleTask);

    } else {
      cachedProbeSide.reset();
    }

    final IT1 probeSideRecord = taskContext.<IT1>getInputSerializer(0).createInstance();
    final IT2 buildSideRecord = taskContext.<IT2>getInputSerializer(1).createInstance();

    while (running && cachedProbeSide.next(probeSideRecord)) {
      MutableHashTable.HashBucketIterator<IT2, IT1> bucket = hashJoin.getMatchesFor(probeSideRecord);
      while (bucket.next(buildSideRecord)) {
        matchStub.match(probeSideRecord, buildSideRecord, collector);
      }
    }

  }

  @Override
  public void cleanup() throws Exception {
    firstIteration = false;
    hashJoin.close();
  }

  public void finalCleanup() {
    hashJoin = null;
    cachedProbeSide = null;
  }

  @Override
  public void cancel() {
    running = false;
    if (hashJoin != null) {
      hashJoin.close();
    }
  }
}