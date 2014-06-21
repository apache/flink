---
title:  "Java API Examples"
---

The following example programs showcase different applications of Stratosphere 
from simple word counting to graph algorithms. The code samples illustrate the 
use of **[Stratosphere's Java API]({{site.baseurl}}/docs/{{site current_stable}}/programming_guides/java.html)**. 

The full source code of the following and more examples can be found in the **[stratosphere-java-examples](https://github.com/stratosphere/stratosphere/tree/release-{{site.current_stable}}/stratosphere-examples/stratosphere-java-examples)** module.

# Word Count
WordCount is the "Hello World" of Big Data processing systems. It computes the frequency of words in a text collection. The algorithm works in two steps: First, the texts are splits the text to individual words. Second, the words are grouped and counted.

```java
// get input data
DataSet<String> text = getTextDataSet(env);

DataSet<Tuple2<String, Integer>> counts = 
        // split up the lines in pairs (2-tuples) containing: (word,1)
        text.flatMap(new Tokenizer())
        // group by the tuple field "0" and sum up tuple field "1"
        .groupBy(0)
        .aggregate(Aggregations.SUM, 1);

counts.writeAsCsv(outputPath, "\n", " ");

// User-defined functions
public static final class Tokenizer extends FlatMapFunction<String, Tuple2<String, Integer>> {

    @Override
    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
        // normalize and split the line
        String[] tokens = value.toLowerCase().split("\\W+");
        
        // emit the pairs
        for (String token : tokens) {
            if (token.length() > 0) {
                out.collect(new Tuple2<String, Integer>(token, 1));
            }   
        }
    }
}
```

The [WordCount example](https://github.com/stratosphere/stratosphere/blob/release-{{site.current_stable}}/stratosphere-examples/stratosphere-java-examples/src/main/java/eu/stratosphere/example/java/wordcount/WordCount.java) implements the above described algorithm with input parameters: `<text input path>, <output path>`. As test data, any text file will do.

# Page Rank

The PageRank algorithm computes the "importance" of pages in a graph defined by links, which point from one pages to another page. It is an iterative graph algorithm, which means that it repeatedly applies the same computation. In each iteration, each page distributes its current rank over all its neighbors, and compute its new rank as a taxed sum of the ranks it received from its neighbors. The PageRank algorithm was popularized by the Google search engine which uses the importance of webpages to rank the results of search queries.

In this simple example, PageRank is implemented with a [bulk iteration]({{site.baseurl}}/docs/{{site.current_stable}}/programming_guides/java.html#iterations) and a fixed number of iterations.

```java
// get input data
DataSet<Tuple2<Long, Double>> pagesWithRanks = getPagesWithRanksDataSet(env);
DataSet<Tuple2<Long, Long[]>> pageLinkLists = getLinksDataSet(env);

// set iterative data set
IterativeDataSet<Tuple2<Long, Double>> iteration = pagesWithRanks.iterate(maxIterations);

DataSet<Tuple2<Long, Double>> newRanks = iteration
        // join pages with outgoing edges and distribute rank
        .join(pageLinkLists).where(0).equalTo(0).flatMap(new JoinVertexWithEdgesMatch())
        // collect and sum ranks
        .groupBy(0).aggregate(SUM, 1)
        // apply dampening factor
        .map(new Dampener(DAMPENING_FACTOR, numPages));

DataSet<Tuple2<Long, Double>> finalPageRanks = iteration.closeWith(
        newRanks, 
        newRanks.join(iteration).where(0).equalTo(0)
        // termination condition
        .filter(new EpsilonFilter()));

finalPageRanks.writeAsCsv(outputPath, "\n", " ");

// User-defined functions

public static final class JoinVertexWithEdgesMatch 
                    extends FlatMapFunction<Tuple2<Tuple2<Long, Double>, Tuple2<Long, Long[]>>, 
                                            Tuple2<Long, Double>> {

    @Override
    public void flatMap(Tuple2<Tuple2<Long, Double>, Tuple2<Long, Long[]>> value, 
                        Collector<Tuple2<Long, Double>> out) {
        Long[] neigbors = value.f1.f1;
        double rank = value.f0.f1;
        double rankToDistribute = rank / ((double) neigbors.length);
            
        for (int i = 0; i < neigbors.length; i++) {
            out.collect(new Tuple2<Long, Double>(neigbors[i], rankToDistribute));
        }
    }
}

public static final class Dampener extends MapFunction<Tuple2<Long,Double>, Tuple2<Long,Double>> {
    private final double dampening, randomJump;

    public Dampener(double dampening, double numVertices) {
        this.dampening = dampening;
        this.randomJump = (1 - dampening) / numVertices;
    }

    @Override
    public Tuple2<Long, Double> map(Tuple2<Long, Double> value) {
        value.f1 = (value.f1 * dampening) + randomJump;
        return value;
    }
}

public static final class EpsilonFilter 
                    extends FilterFunction<Tuple2<Tuple2<Long, Double>, Tuple2<Long, Double>>> {

    @Override
    public boolean filter(Tuple2<Tuple2<Long, Double>, Tuple2<Long, Double>> value) {
        return Math.abs(value.f0.f1 - value.f1.f1) > EPSILON;
    }
}
```

The [PageRank program](https://github.com/stratosphere/stratosphere/blob/release-{{site.current_stable}}/stratosphere-examples/stratosphere-java-examples/src/main/java/eu/stratosphere/example/java/graph/PageRankBasic.java) implements the above example.
It requires the following parameters to run: `<pages input path>, <links input path>, <output path>, <num pages>, <num iterations>`.

Input files are plain text files and must be formatted as follows:
- Pages represented as an (long) ID separated by new-line characters.
    * For example `"1\n2\n12\n42\n63\n"` gives five pages with IDs 1, 2, 12, 42, and 63.
- Links are represented as pairs of page IDs which are separated by space characters. Links are separated by new-line characters:
    * For example `"1 2\n2 12\n1 12\n42 63\n"` gives four (directed) links (1)->(2), (2)->(12), (1)->(12), and (42)->(63).

For this simple implementation it is required that each page has at least one incoming and one outgoing link (a page can point to itself).

# Connected Components

The Connected Components algorithm identifies parts of a larger graph which are connected by assigning all vertices in the same connected part the same component ID. Similar to PageRank, Connected Components is an iterative algorithm. In each step, each vertex propagates its current component ID to all its neighbors. A vertex accepts the component ID from a neighbor, if it is smaller than its own component ID.

This implementation uses a [delta iteration]({{site.baseurl}}/docs/{{site.current_stable}}/programming_guides/java.html#iterations): Vertices that have not changed their component ID do not participate in the next step. This yields much better performance, because the later iterations typically deal only with a few outlier vertices.

```java
// read vertex and edge data
DataSet<Long> vertices = getVertexDataSet(env);
DataSet<Tuple2<Long, Long>> edges = getEdgeDataSet(env).flatMap(new UndirectEdge());

// assign the initial component IDs (equal to the vertex ID)
DataSet<Tuple2<Long, Long>> verticesWithInitialId = vertices.map(new DuplicateValue<Long>());
        
// open a delta iteration
DeltaIteration<Tuple2<Long, Long>, Tuple2<Long, Long>> iteration =
        verticesWithInitialId.iterateDelta(verticesWithInitialId, maxIterations, 0);

// apply the step logic: 
DataSet<Tuple2<Long, Long>> changes = iteration.getWorkset()
        // join with the edges
        .join(edges).where(0).equalTo(0).with(new NeighborWithComponentIDJoin())
        // select the minimum neighbor component ID
        .groupBy(0).aggregate(Aggregations.MIN, 1)
        // update if the component ID of the candidate is smaller
        .join(iteration.getSolutionSet()).where(0).equalTo(0)
        .flatMap(new ComponentIdFilter());

// close the delta iteration (delta and new workset are identical)
DataSet<Tuple2<Long, Long>> result = iteration.closeWith(changes, changes);

// emit result
result.writeAsCsv(outputPath, "\n", " ");

// User-defined functions

public static final class DuplicateValue<T> extends MapFunction<T, Tuple2<T, T>> {
    
    @Override
    public Tuple2<T, T> map(T vertex) {
        return new Tuple2<T, T>(vertex, vertex);
    }
}

public static final class UndirectEdge 
                    extends FlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {
    Tuple2<Long, Long> invertedEdge = new Tuple2<Long, Long>();
    
    @Override
    public void flatMap(Tuple2<Long, Long> edge, Collector<Tuple2<Long, Long>> out) {
        invertedEdge.f0 = edge.f1;
        invertedEdge.f1 = edge.f0;
        out.collect(edge);
        out.collect(invertedEdge);
    }
}

public static final class NeighborWithComponentIDJoin 
                    extends JoinFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>> {

    @Override
    public Tuple2<Long, Long> join(Tuple2<Long, Long> vertexWithComponent, Tuple2<Long, Long> edge) {
        return new Tuple2<Long, Long>(edge.f1, vertexWithComponent.f1);
    }
}

public static final class ComponentIdFilter 
                    extends FlatMapFunction<Tuple2<Tuple2<Long, Long>, Tuple2<Long, Long>>, 
                                            Tuple2<Long, Long>> {

    @Override
    public void flatMap(Tuple2<Tuple2<Long, Long>, Tuple2<Long, Long>> value, 
                        Collector<Tuple2<Long, Long>> out) {
        if (value.f0.f1 < value.f1.f1) {
            out.collect(value.f0);
        }
    }
}
```

The [ConnectedComponents program](https://github.com/stratosphere/stratosphere/blob/release-{{site.current_stable}}/stratosphere-examples/stratosphere-java-examples/src/main/java/eu/stratosphere/example/java/graph/ConnectedComponents.java) implements the above example. It requires the following parameters to run: `<vertex input path>, <edge input path>, <output path> <max num iterations>`.

Input files are plain text files and must be formatted as follows:
- Vertices represented as IDs and separated by new-line characters.
    * For example `"1\n2\n12\n42\n63\n"` gives five vertices with (1), (2), (12), (42), and (63).
- Edges are represented as pairs for vertex IDs which are separated by space characters. Edges are separated by new-line characters:
    * For example `"1 2\n2 12\n1 12\n42 63\n"` gives four (undirected) links (1)-(2), (2)-(12), (1)-(12), and (42)-(63).

# Relational Query

The Relational Query example assumes two tables, one with `orders` and the other with `lineitems` as specified by the [TPC-H decision support benchmark](http://www.tpc.org/tpch/). TPC-H is a standard benchmark in the database industry. See below for instructions how to generate the input data.

The example implements the following SQL query.

```sql
SELECT l_orderkey, o_shippriority, sum(l_extendedprice) as revenue
    FROM orders, lineitem
WHERE l_orderkey = o_orderkey
    AND o_orderstatus = "F" 
    AND YEAR(o_orderdate) > 1993
    AND o_orderpriority LIKE "5%"
GROUP BY l_orderkey, o_shippriority;
```

The Stratosphere Java program, which implements the above query looks as follows.

```java
// get orders data set: (orderkey, orderstatus, orderdate, orderpriority, shippriority)
DataSet<Tuple5<Integer, String, String, String, Integer>> orders = getOrdersDataSet(env);
// get lineitem data set: (orderkey, extendedprice)
DataSet<Tuple2<Integer, Double>> lineitems = getLineitemDataSet(env);

// orders filtered by year: (orderkey, custkey)
DataSet<Tuple2<Integer, Integer>> ordersFilteredByYear =
        // filter orders
        orders.filter(
            new FilterFunction<Tuple5<Integer, String, String, String, Integer>>() {
                @Override
                public boolean filter(Tuple5<Integer, String, String, String, Integer> t) {
                    // status filter
                    if(!t.f1.equals(STATUS_FILTER)) {
                        return false;
                    // year filter
                    } else if(Integer.parseInt(t.f2.substring(0, 4)) <= YEAR_FILTER) {
                        return false;
                    // order priority filter
                    } else if(!t.f3.startsWith(OPRIO_FILTER)) {
                        return false;
                    }
                    return true;
                }
            })
        // project fields out that are no longer required
        .project(0,4).types(Integer.class, Integer.class);

// join orders with lineitems: (orderkey, shippriority, extendedprice)
DataSet<Tuple3<Integer, Integer, Double>> lineitemsOfOrders = 
        ordersFilteredByYear.joinWithHuge(lineitems)
                            .where(0).equalTo(0)
                            .projectFirst(0,1).projectSecond(1)
                            .types(Integer.class, Integer.class, Double.class);

// extendedprice sums: (orderkey, shippriority, sum(extendedprice))
DataSet<Tuple3<Integer, Integer, Double>> priceSums = 
        // group by order and sum extendedprice
        lineitemsOfOrders.groupBy(0,1).aggregate(Aggregations.SUM, 2);

// emit result
priceSums.writeAsCsv(outputPath);
```

The [Relational Query program](https://github.com/stratosphere/stratosphere/blob/release-{{site.current_stable}}/stratosphere-examples/stratosphere-java-examples/src/main/java/eu/stratosphere/example/java/relational/RelationalQuery.java) implements the above query. It requires the following parameters to run: `<orders input path>, <lineitem input path>, <output path>`.

The orders and lineitem files can be generated using the [TPC-H benchmark](http://www.tpc.org/tpch/) suite's data generator tool (DBGEN). 
Take the following steps to generate arbitrary large input files for the provided Stratosphere programs:

1.  Download and unpack DBGEN
2.  Make a copy of *makefile.suite* called *Makefile* and perform the following changes:

```bash
DATABASE = DB2
MACHINE  = LINUX
WORKLOAD = TPCH
CC       = gcc
```

1.  Build DBGEN using *make*
2.  Generate lineitem and orders relations using dbgen. A scale factor
    (-s) of 1 results in a generated data set with about 1 GB size.

```bash
./dbgen -T o -s 1
```