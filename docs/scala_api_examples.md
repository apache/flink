---
title:  "Scala API Examples"
---

The following example programs showcase different applications of Stratosphere from simple word counting to graph algorithms.
The code samples illustrate the use of [Stratosphere's Scala API](scala_api_guide.html). 

The full source code of the following and more examples can be found in the [stratosphere-scala-examples](https://github.com/apache/incubator-flink/tree/ca2b287a7a78328ebf43766b9fdf39b56fb5fd4f/stratosphere-examples/stratosphere-scala-examples) module.

# Word Count

WordCount is the "Hello World" of Big Data processing systems. It computes the frequency of words in a text collection. The algorithm works in two steps: First, the texts are splits the text to individual words. Second, the words are grouped and counted.

```scala
// read input data
val input = TextFile(textInput)

// tokenize words
val words = input.flatMap { _.split(" ") map { (_, 1) } }

// count by word
val counts = words.groupBy { case (word, _) => word }
  .reduce { (w1, w2) => (w1._1, w1._2 + w2._2) }

val output = counts.write(wordsOutput, CsvOutputFormat()))
```

The {% gh_link /stratosphere-examples/stratosphere-scala-examples/src/main/scala/eu/stratosphere/examples/scala/wordcount/WordCount.scala "WordCount example" %} implements the above described algorithm with input parameters: `<degree of parallelism>, <text input path>, <output path>`. As test data, any text file will do.

# Page Rank

The PageRank algorithm computes the "importance" of pages in a graph defined by links, which point from one pages to another page. It is an iterative graph algorithm, which means that it repeatedly applies the same computation. In each iteration, each page distributes its current rank over all its neighbors, and compute its new rank as a taxed sum of the ranks it received from its neighbors. The PageRank algorithm was popularized by the Google search engine which uses the importance of webpages to rank the results of search queries.

In this simple example, PageRank is implemented with a [bulk iteration](java_api_guide.html#iterations) and a fixed number of iterations.

```scala
// cases classes so we have named fields
case class PageWithRank(pageId: Long, rank: Double)
case class Edge(from: Long, to: Long, transitionProbability: Double)

// constants for the page rank formula
val dampening = 0.85
val randomJump = (1.0 - dampening) / NUM_VERTICES
val initialRank = 1.0 / NUM_VERTICES
  
// read inputs
val pages = DataSource(verticesPath, CsvInputFormat[Long]())
val edges = DataSource(edgesPath, CsvInputFormat[Edge]())

// assign initial rank
val pagesWithRank = pages map { p => PageWithRank(p, initialRank) }

// the iterative computation
def computeRank(ranks: DataSet[PageWithRank]) = {

    // send rank to neighbors
    val ranksForNeighbors = ranks join edges
        where { _.pageId } isEqualTo { _.from }
        map { (p, e) => (e.to, p.rank * e.transitionProbability) }
    
    // gather ranks per vertex and apply page rank formula
    ranksForNeighbors .groupBy { case (node, rank) => node }
                      .reduce { (a, b) => (a._1, a._2 + b._2) }
                      .map {case (node, rank) => PageWithRank(node, rank * dampening + randomJump) }
}

// invoke iteratively
val finalRanks = pagesWithRank.iterate(numIterations, computeRank)
val output = finalRanks.write(outputPath, CsvOutputFormat())
```



The {% gh_link /stratosphere-examples/stratosphere-scala-examples/src/main/scala/eu/stratosphere/examples/scala/graph/PageRank.scala "PageRank program" %} implements the above example.
It requires the following parameters to run: `<pages input path>, <link input path>, <output path>, <num pages>, <num iterations>`.

Input files are plain text files and must be formatted as follows:
- Pages represented as an (long) ID separated by new-line characters.
    * For example `"1\n2\n12\n42\n63\n"` gives five pages with IDs 1, 2, 12, 42, and 63.
- Links are represented as pairs of page IDs which are separated by space characters. Links are separated by new-line characters:
    * For example `"1 2\n2 12\n1 12\n42 63\n"` gives four (directed) links (1)->(2), (2)->(12), (1)->(12), and (42)->(63).

For this simple implementation it is required that each page has at least one incoming and one outgoing link (a page can point to itself).

# Connected Components

The Connected Components algorithm identifies parts of a larger graph which are connected by assigning all vertices in the same connected part the same component ID. Similar to PageRank, Connected Components is an iterative algorithm. In each step, each vertex propagates its current component ID to all its neighbors. A vertex accepts the component ID from a neighbor, if it is smaller than its own component ID.

This implementation uses a [delta iteration](iterations.html): Vertices that have not changed their component id do not participate in the next step. This yields much better performance, because the later iterations typically deal only with a few outlier vertices.

```scala
// define case classes
case class VertexWithComponent(vertex: Long, componentId: Long)
case class Edge(from: Long, to: Long)

// get input data
val vertices = DataSource(verticesPath, CsvInputFormat[Long]())
val directedEdges = DataSource(edgesPath, CsvInputFormat[Edge]())

// assign each vertex its own ID as component ID
val initialComponents = vertices map { v => VertexWithComponent(v, v) }
val undirectedEdges = directedEdges flatMap { e => Seq(e, Edge(e.to, e.from)) }

def propagateComponent(s: DataSet[VertexWithComponent], ws: DataSet[VertexWithComponent]) = {
  val allNeighbors = ws join undirectedEdges
        where { _.vertex } isEqualTo { _.from }
        map { (v, e) => VertexWithComponent(e.to, v.componentId ) }
    
    val minNeighbors = allNeighbors groupBy { _.vertex } reduceGroup { cs => cs minBy { _.componentId } }

    // updated solution elements == new workset
    val s1 = s join minNeighbors
        where { _.vertex } isEqualTo { _.vertex }
        flatMap { (curr, candidate) =>
            if (candidate.componentId < curr.componentId) Some(candidate) else None
        }

  (s1, s1)
}

val components = initialComponents.iterateWithDelta(initialComponents, { _.vertex }, propagateComponent,
                    maxIterations)
val output = components.write(componentsOutput, CsvOutputFormat())
```

The {% gh_link /stratosphere-examples/stratosphere-scala-examples/src/main/scala/eu/stratosphere/examples/scala/graph/ConnectedComponents.scala "ConnectedComponents program" %} implements the above example. It requires the following parameters to run: `<vertex input path>, <edge input path>, <output path> <max num iterations>`.

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

The Stratosphere Scala program, which implements the above query looks as follows.

```scala
// --- define some custom classes to address fields by name ---
case class Order(orderId: Int, status: Char, date: String, orderPriority: String, shipPriority: Int)
case class LineItem(orderId: Int, extendedPrice: Double)
case class PrioritizedOrder(orderId: Int, shipPriority: Int, revenue: Double)

val orders = DataSource(ordersInputPath, DelimitedInputFormat(parseOrder))
val lineItem2600s = DataSource(lineItemsInput, DelimitedInputFormat(parseLineItem))

val filteredOrders = orders filter { o => o.status == "F" && o.date.substring(0, 4).toInt > 1993 && o.orderPriority.startsWith("5") }

val prioritizedItems = filteredOrders join lineItems
    where { _.orderId } isEqualTo { _.orderId } // join on the orderIds
    map { (o, li) => PrioritizedOrder(o.orderId, o.shipPriority, li.extendedPrice) }

val prioritizedOrders = prioritizedItems
    groupBy { pi => (pi.orderId, pi.shipPriority) } 
    reduce { (po1, po2) => po1.copy(revenue = po1.revenue + po2.revenue) }

val output = prioritizedOrders.write(ordersOutput, CsvOutputFormat(formatOutput))
```

The {% gh_link /stratosphere-examples/stratosphere-scala-examples/src/main/scala/eu/stratosphere/examples/scala/relational/RelationalQuery.scala "Relational Query program" %} implements the above query. It requires the following parameters to run: `<orders input path>, <lineitem input path>, <output path>, <degree of parallelism>`.

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