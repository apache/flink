---
title:  "How to add a new Operator"
---

Operators in the Java API can be added in multiple different ways: 

1. On the DataSet, as a specialization/combination of existing operators
2. As a custom extension operator
3. As a new runtime operator

The first two approaches are typically more lightweight and easier to implement. Sometimes,
new functionality does require a new runtime operator, or it is much more efficient to 


## Implementing a new Operator on DataSet

Many operators can be implemented as a specialization of another operator, or by means of a UDF.

The simplest example are the `sum()`, `min()`, and `max()` functions on the {% gh_link /stratosphere-java/src/main/java/eu/stratosphere/api/java/DataSet.java "DataSet" %}. These functions simply call other operations
with some pre-defined parameters:
```
public AggregateOperator<T> sum (int field) {
    return this.aggregate (Aggregations.SUM, field);
}

```

Some operations can be implemented as compositions of multiple other operators. An example is to implement a
*count()* function through a combination of *map* and *aggregate*. 

A simple way to do this is to define a function on the {% gh_link /stratosphere-java/src/main/java/eu/stratosphere/api/java/DataSet.java "DataSet" %} that calls *map(...)* and *reduce(...)* in turn:
```
public DataSet<Long> count() {
    return this.map(new MapFunction<T, Long>() {
                        public Long map(T value) {
                            return 1L;
                        }
                    })
               .reduce(new ReduceFunction<Long>() {
                        public Long reduce(Long val1, Long val1) {
                            return val1 + val2;
                        }
                    });
}
```

To define a new operator without altering the DataSet class is possible by putting the functions as static members
into another class. The example of the *count()* operator would look the following way:
```
public static <T>DataSet<Long> count(DataSet<T> data) {
    return data.map(...).reduce(...);
}
```

### More Complex Operators

A more complex example of an operation via specialization is the {% gh_link /stratosphere-java/src/main/java/eu/stratosphere/api/java/operators/AggregateOperator.java "Aggregation Operation" %} in the Java API. It is implemented by means of a *GroupReduce* UDF.

The Aggregate Operation comes with its own operator in the *Java API*, but translates itself into a {% gh_link /stratosphere-core/src/main/java/eu/stratosphere/api/common/operators/base/GroupReduceOperatorBase.java "GroupReduceOperatorBase" %} in the *Common API*. (see [Program Life Cycle](program_life_cycle.html) for details of how an operation from the *Java API* becomes an operation of the *Common API* and finally a runtime operation.)
The Java API aggregation operator is only a builder that takes the types of aggregations and the field positions, and used that information to
parameterize the GroupReduce UDF that performs the aggregations.

Because the operation is translated into a GroupReduce operation, it appears as a GroupReduceOperator in the optimizer and runtime.



## Implementing a Custom Extension Operator

The DataSet offers a method for custom operators: `DataSet<X> runOperation(CustomUnaryOperation<T, X> operation)`.
The *CustomUnaryOperation* interface defines operators by means of the two functions:
``` java
void setInput(DataSet<IN> inputData);
	
DataSet<OUT> createResult();
```

The {% gh_link /stratosphere-addons/spargel/src/main/java/eu/stratosphere/spargel/java/VertexCentricIteration.java "VertexCentricIteration" %} operator is implemented that way. Below is an example how to implement the *count()* operator that way.

``` java
public class Counter<T> implements CustomUnaryOperation<T, Long> {

    private DataSet<T> input;

    public void setInput(DataSet<IN> inputData) { this.input = inputData; }

    public DataSet<Long> createResult() {
        return input.map(...).reduce(...);
    }
}
```
The CountOperator can be called in the following way:
``` java
DataSet<String> lines = ...;
DataSet<Long> count = lines.runOperation(new Counter<String>());
```


## Implementing a new Runtime Operator

Adding an new runtime operator requires changes throughout the entire stack, from the API to the runtime:

- *Java API*
- *Common API*
- *Optimizer*
- *Runtime*

We start the description bottom up, at the example of the *mapPartition()* function, which is like a *map*
function, but invoked only once per parallel partition.

**Runtime**

Runtime Operators are implemented using the {% gh_link /stratosphere-runtime/src/main/java/eu/stratosphere/pact/runtime/task/PactDriver.java "Driver" %} interface. The interface defines the methods that describe the operator towards the runtime. The {% gh_link /stratosphere-runtime/src/main/java/eu/stratosphere/pact/runtime/task/MapDriver.java "MapDriver" %} serves as a simple example of how those operators work.

The runtime works with the `MutableObjectIterator`, which describes data streams with the ability to reuse objects, to reduce pressure on the garbage collector.

An implementation of the central `run()` method for the *mapPartition* operator could look the following way:
``` java
public void run() throws Exception {
    final MutableObjectIterator<IN> input = this.taskContext.getInput(0);
    final MapPartitionFunction<IN, OUT> function = this.taskContext.getStub();
    final Collector<OUT> output = this.taskContext.getOutputCollector();
    final TypeSerializer<IN> serializer = this.taskContext.getInputSerializer(0);

    // we assume that the UDF takes a java.util.Iterator, so we wrap the MutableObjectIterator
    Iterator<IN> iterator = new MutableToRegularIteratorWrapper(input, serializer);

    function.mapPartition(iterator, output);
}
```

To increase efficiency, it is often beneficial to implement a *chained* version of an operator. Chained
operators run in the same thread as their preceding operator, and work with nested function calls.
This is very efficient, because it saves serialization/deserialization overhead.

To learn how to implement a chained operator, take a look at the {% gh_link /stratosphere-runtime/src/main/java/eu/stratosphere/pact/runtime/task/MapDriver.java "MapDriver" %} (regular) and the
{% gh_link /stratosphere-runtime/src/main/java/eu/stratosphere/pact/runtime/task/chaining/ChainedMapDriver.java "ChainedMapDriver" %} (chained variant).


**Optimizer/Compiler**

This section does a minimal discussion of the important steps to add an operator. Please see the [Optimizer](optimizer.html) docs for more detail on how the optimizer works.
To allow the optimizer to include a new operator in its planning, it needs a bit of information about it; in particular, the following information:

- *{% gh_link /stratosphere-runtime/src/main/java/eu/stratosphere/pact/runtime/task/DriverStrategy.java "DriverStrategy" %}*: The operation needs to be added to the Enum, to make it available to the optimizer. The parameters to the Enum entry define which class implements the runtime operator, its chained version, whether the operator accumulates records (and needs memory for that), and whether it requires a comparator (works on keys). For our example, we can add the entry
``` java
MAP_PARTITION(MapPartitionDriver.class, null /* or chained variant */, PIPELINED, false)
```

- *Cost function*: The class {% gh_link /stratosphere-compiler/src/main/java/eu/stratosphere/compiler/costs/CostEstimator.java "CostEstimator" %} needs to know how expensive the operation is to the system. The costs here refer to the non-UDF part of the operator. Since the operator does essentially no work (it forwards the record stream to the UDF), the costs are zero. We change the `costOperator(...)` method by adding the *MAP_PARTITION* constant to the switch statement similar to the *MAP* constant such that no cost is accounted for it.

- *OperatorDescriptor*: The operator descriptors define how an operation needs to be treated by the optimizer. They describe how the operation requires the input data to be (e.g., sorted or partitioned) and that way allows the optimizer to optimize the data movement, sorting, grouping in a global fashion. They do that by describing which {% gh_link /stratosphere-compiler/src/main/java/eu/stratosphere/compiler/dataproperties/RequestedGlobalProperties.java "RequestedGlobalProperties" %} (partitioning, replication, etc) and which {% gh_link /stratosphere-compiler/src/main/java/eu/stratosphere/compiler/dataproperties/RequestedLocalProperties.java "RequestedLocalProperties" %} (sorting, grouping, uniqueness) the operator has, as well as how the operator affects the existing {% gh_link /stratosphere-compiler/src/main/java/eu/stratosphere/compiler/dataproperties/GlobalProperties.java "GlobalProperties" %} and {% gh_link /stratosphere-compiler/src/main/java/eu/stratosphere/compiler/dataproperties/LocalProperties.java "LocalProperties" %}. In addition, it defines a few utility methods, for example to instantiate an operator candidate.
Since the *mapPartition()* function is very simple (no requirements on partitioning/grouping), the descriptor is very simple. Other operators have more complex requirements, for example the {% gh_link /stratosphere-compiler/src/main/java/eu/stratosphere/compiler/operators/GroupReduceProperties.java "GroupReduce" %}. Some operators, like *join* have multiple ways in which they can be executed and therefore have multiple descriptors ({% gh_link /stratosphere-compiler/src/main/java/eu/stratosphere/compiler/operators/HashJoinBuildFirstProperties.java "Hash Join 1" %}, {% gh_link /stratosphere-compiler/src/main/java/eu/stratosphere/compiler/operators/HashJoinBuildSecondProperties.java "Hash Join 2" %}, {% gh_link /stratosphere-compiler/src/main/java/eu/stratosphere/compiler/operators/SortMergeJoinDescriptor.java "SortMerge Join" %}).
The code sample below explains (with comments) how to create a descriptor for the *MapPartitionOperator*
``` java
    public DriverStrategy getStrategy() {
        return MAP_PARTITION;
    }

    // Instantiate the operator with the strategy over the input given in the form of the Channel
    public SingleInputPlanNode instantiate(Channel in, SingleInputNode node) {
        return new SingleInputPlanNode(node, "MapPartition", in, MAP_PARTITION);
    }

    // The operation accepts data with default global properties (arbitrary distribution)
    protected List<RequestedGlobalProperties> createPossibleGlobalProperties() {
        return Collections.singletonList(new RequestedGlobalProperties());
    }

    // The operation can accept data with any local properties. No grouping/sorting is necessary
    protected List<RequestedLocalProperties> createPossibleLocalProperties() {
        return Collections.singletonList(new RequestedLocalProperties());
    }

    // the operation itself does not affect the existing global properties.
    // The effect of the UDF's semantics// are evaluated separately (by interpreting the
    // semantic assertions)
    public GlobalProperties computeGlobalProperties(GlobalProperties gProps) {
        return gProps;
    }

    // since the operation can mess up all order, grouping, uniqueness, we cannot make any statements
    // about how local properties are preserved
    public LocalProperties computeLocalProperties(LocalProperties lProps) {
        return LocalProperties.EMPTY;
    }
```

- *OptimizerNode*: The optimizer node is the place where all comes together. It creates the list of *OperatorDescriptors*, implements the result data set size estimation, and assigns a name to the operation. It is a relatively small class and can be more or less copied again from the {% gh_link /stratosphere-compiler/src/main/java/eu/stratosphere/compiler/dag/MapNode.java "MapNode" %}.


**Common API**

To make the operation available to the higher-level APIs, it needs to be added to the Common API. The simplest way to do this is to add a
base operator. Create a class `MapPartitionOperatorBase`, after the pattern of the {% gh_link /stratosphere-core/src/main/java/eu/stratosphere/api/common/operators/base/MapOperatorBase.java "MapOperatorBase" %}.

In addition, the optimizer needs to know which OptimizerNode how to create an OptimizerNode from the OperatorBase. This happens in the class
`GraphCreatingVisitor` in the {% gh_link /stratosphere-compiler/src/main/java/eu/stratosphere/compiler/PactCompiler.java "Optimizer" %}.

*Note:* A pending idea is to allow to skip this step by unifying the OptimizerNode and the Common API operator. They essentially fulfill the
same function. The Common API operator exists only in order for the `flink-java` and `flink-scala` packages to not have a dependency on the
optimizer.


**Java API**

Create a Java API operator that is constructed in the same way as the {% gh_link /stratosphere-java/src/main/java/eu/stratosphere/api/java/operators/MapOperator.java "MapOperator" %}. The core method is the `translateToDataFlow(...)` method, which creates the Common API operator for the Java API operator.

The final step is to add a function to the `DataSet` class:
``` java
public <R> DataSet<R> mapPartition(MapPartitionFunction<T, R> function) {
    return new MapPartitionOperator<T, R>(this, function);
}
```


