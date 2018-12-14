---
title: 'Detecting Patterns in Tables'
nav-parent_id: streaming_tableapi
nav-title: 'Detecting Patterns'
nav-pos: 5
is_beta: true
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

It is a common use case to search for a set of event patterns, especially in case of data streams. Flink
comes with a [complex event processing (CEP) library]({{ site.baseurl }}/dev/libs/cep.html) which allows for pattern detection in event streams. Furthermore, Flink's
SQL API provides a relational way of expressing queries with a large set of built-in functions and rule-based optimizations that can be used out of the box.

In December 2016, the International Organization for Standardization (ISO) released a new version of the SQL standard which includes _Row Pattern Recognition in SQL_ ([ISO/IEC TR 19075-5:2016](https://standards.iso.org/ittf/PubliclyAvailableStandards/c065143_ISO_IEC_TR_19075-5_2016.zip)). It allows Flink to consolidate CEP and SQL API using the `MATCH_RECOGNIZE` clause for complex event processing in SQL.

A `MATCH_RECOGNIZE` clause enables the following tasks:
* Logically partition and order the data that is used with the `PARTITION BY` and `ORDER BY` clauses.
* Define patterns of rows to seek using the `PATTERN` clause. These patterns use a syntax similar to that of regular expressions.
* The logical components of the row pattern variables are specified in the `DEFINE` clause.
* Define measures, which are expressions usable in other parts of the SQL query, in the `MEASURES` clause.

The following example illustrates the syntax for basic pattern recognition:

{% highlight sql %}
SELECT T.aid, T.bid, T.cid
FROM MyTable
MATCH_RECOGNIZE (
  PARTITION BY userid
  ORDER BY proctime
  MEASURES
    A.id AS aid,
    B.id AS bid,
    C.id AS cid
  PATTERN (A B C)
  DEFINE
    A AS name = 'a',
    B AS name = 'b',
    C AS name = 'c'
) AS T
{% endhighlight %}

This page will explain each keyword in more detail and will illustrate more complex examples.

<span class="label label-danger">Attention</span> Flink's implementation of the `MATCH_RECOGNIZE` clause is a subset of the full standard. Only those features documented in the following sections are supported. Since the development is still in an early phase, please also take a look at the [known limitations](#known-limitations).

* This will be replaced by the TOC
{:toc}

Introduction and Examples
-------------------------

### Installation Guide

The pattern recognition feature uses the Apache Flink's CEP library internally. In order to be able to use the `MATCH_RECOGNIZE` clause,
the library needs to be added as a dependency to your Maven project.

{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-cep{{ site.scala_version_suffix }}</artifactId>
  <version>{{ site.version }}</version>
</dependency>
{% endhighlight %}

Alternatively, you can also add the dependency to the cluster classpath (see the [dependency section]({{ site.baseurl}}/projectsetup/dependencies.html) for more information).

If you want to use the `MATCH_RECOGNIZE` clause in the [SQL Client]({{ site.baseurl}}/dev/table/sqlClient.html),
you don't have to do anything as all the dependencies are included by default.

### SQL Semantics

Every `MATCH_RECOGNIZE` query consists of the following clauses:

* [PARTITION BY](#partitioning) - defines the logical partitioning of the table; similar to a `GROUP BY` operation.
* [ORDER BY](#order-of-events) - specifies how the incoming rows should be ordered; this is essential as patterns depend on an order.
* [MEASURES](#define--measures) - defines output of the clause; similar to a `SELECT` clause.
* [ONE ROW PER MATCH](#output-mode) - output mode which defines how many rows per match should be produced.
* [AFTER MATCH SKIP](#after-match-strategy) - specifies where the next match should start; this is also a way to control how many distinct matches a single event can belong to.
* [PATTERN](#defining-pattern) - allows constructing patterns that will be searched for using a _regular expression_-like syntax.
* [DEFINE](#define--measures) - this section defines the conditions that the pattern variables must satisfy.

<span class="label label-danger">Attention</span> Currently, the `MATCH_RECOGNIZE` clause can only be applied to an [append table](dynamic_tables.html#update-and-append-queries). Furthermore, it always produces
an append table as well.

### Examples

For our examples, we assume that a table `Ticker` has been registered. The table contains prices of stocks at a particular point in time.

The table has a following schema:

{% highlight text %}
Ticker
     |-- symbol: String                           # symbol of the stock
     |-- price: Long                              # price of the stock
     |-- tax: Long                                # tax liability of the stock
     |-- rowtime: TimeIndicatorTypeInfo(rowtime)  # point in time when the change to those values happened
{% endhighlight %}

For simplification, we only consider the incoming data for a single stock `ACME`. A ticker could look similar to the following table where rows are continuously appended.

{% highlight text %}
symbol         rowtime         price    tax
======  ====================  ======= =======
'ACME'  '01-Apr-11 10:00:00'   12      1
'ACME'  '01-Apr-11 10:00:01'   17      2
'ACME'  '01-Apr-11 10:00:02'   19      1
'ACME'  '01-Apr-11 10:00:03'   21      3
'ACME'  '01-Apr-11 10:00:04'   25      2
'ACME'  '01-Apr-11 10:00:05'   18      1
'ACME'  '01-Apr-11 10:00:06'   15      1
'ACME'  '01-Apr-11 10:00:07'   14      2
'ACME'  '01-Apr-11 10:00:08'   24      2
'ACME'  '01-Apr-11 10:00:09'   25      2
'ACME'  '01-Apr-11 10:00:10'   19      1
{% endhighlight %}

The task is now to find periods of a constantly decreasing price of a single ticker. For this, one could write a query like:

{% highlight sql %}
SELECT *
FROM Ticker
MATCH_RECOGNIZE (
    PARTITION BY symbol
    ORDER BY rowtime
    MEASURES
        START_ROW.rowtime AS start_tstamp,
        LAST(PRICE_DOWN.rowtime) AS bottom_tstamp,
        LAST(PRICE_UP.rowtime) AS end_tstamp
    ONE ROW PER MATCH
    AFTER MATCH SKIP TO LAST PRICE_UP
    PATTERN (START_ROW PRICE_DOWN+ PRICE_UP)
    DEFINE
        PRICE_DOWN AS
            (LAST(PRICE_DOWN.price, 1) IS NULL AND PRICE_DOWN.price < START_ROW.price) OR
                PRICE_DOWN.price < LAST(PRICE_DOWN.price, 1),
        PRICE_UP AS
            PRICE_UP.price > LAST(PRICE_DOWN.price, 1)
    ) MR;
{% endhighlight %}

The query partitions the `Ticker` table by the `symbol` column and orders it by the `rowtime` time attribute.

The `PATTERN` clause specifies that we are interested in a pattern with a starting event `START_ROW` that is followed by one or more `PRICE_DOWN` events and concluded with a `PRICE_UP` event. If such a pattern can be found, the next pattern match will be seeked at the last `PRICE_UP` event as indicated by the `AFTER MATCH SKIP TO LAST` clause.

The `DEFINE` clause specifies the conditions that need to be met for a `PRICE_DOWN` and `PRICE_UP` event. Although the `START_ROW` pattern variable is not present it has an implicit condition that is evaluated always as `TRUE`.

A pattern variable `PRICE_DOWN` is defined as a row with a price that is smaller than the price of the last row that met the `PRICE_DOWN` condition. For the initial case or when there is no last row that met the `PRICE_DOWN` condition, the price of the row should be smaller than the price of the preceding row in the pattern (referenced by `START_ROW`).

A pattern variable `PRICE_UP` is defined as a row with a price that is larger than the price of the last row that met the `PRICE_DOWN` condition.

This query produces a summary row for each period in which the price of a stock was continuously decreasing.

The exact representation of the output rows is defined in the `MEASURES` part of the query. The number of output rows is defined by the `ONE ROW PER MATCH` output mode.

{% highlight text %}
 symbol       start_tstamp       bottom_tstamp         end_tstamp
=========  ==================  ==================  ==================
ACME       01-APR-11 10:00:04  01-APR-11 10:00:07  01-APR-11 10:00:08
{% endhighlight %}

The resulting row describes a period of falling prices that started at `01-APR-11 10:00:04` and
achieved the lowest price at `01-APR-11 10:00:07` that increased again at `01-APR-11 10:00:08`.

Partitioning
------------

It is possible to look for patterns in partitioned data, e.g., trends for a single ticker or a particular user. This can be expressed using the `PARTITION BY` clause. The clause is similar to using `GROUP BY` for aggregations.

<span class="label label-danger">Attention</span> It is highly advised to partition the incoming data because otherwise the `MATCH_RECOGNIZE` clause will be translated
into a non-parallel operator to ensure global ordering.

Order of Events
---------------

Apache Flink allows for searching for patterns based on time; either [processing time or event time](time_attributes.html).

In case of event time, the events are sorted before they are passed to the internal pattern state machine. As a consequence, the
produced output will be correct regardless of the order in which rows are appended to the table. Instead, the pattern is evaluated in the order specified by the time contained in each row.

The `MATCH_RECOGNIZE` clause assumes a [time attribute](time_attributes.html) with ascending ordering as the first argument to `ORDER BY` clause.

For the example `Ticker` table, a definition like `ORDER BY rowtime ASC, price DESC` is valid but `ORDER BY price, rowtime` or `ORDER BY rowtime DESC, price ASC` is not.

Define & Measures
-----------------

The `DEFINE` and `MEASURES` keywords have similar meanings to the `WHERE` and `SELECT` clauses in a simple SQL query.

The `MEASURES` clause defines what will be included in the output of a matching pattern. It can project columns and define expressions for evaluation.
The number of produced rows depends on the [output mode](#output-mode) setting.

The `DEFINE` clause specifies conditions that rows have to fulfill in order to be classified to a corresponding [pattern variable](#defining-pattern).
If a condition is not defined for a pattern variable, a default condition will be used which evaluates to `true` for every row.

For a more detailed explanation about expressions that can be used in those clauses, please have a look at the [event stream navigation](#pattern-navigation) section.

Defining a Pattern
------------------

The `MATCH_RECOGNIZE` clause allows users to search for patterns in event streams using a powerful and expressive syntax
that is somewhat similar to the widespread regular expression syntax.

Every pattern is constructed from basic building blocks, called _pattern variables_, to which operators (quantifiers and other modifiers) can be applied. The whole pattern must be enclosed in
brackets.

An example pattern could look like:

{% highlight sql %}
PATTERN (A B+ C* D)
{% endhighlight %}

One may use the following operators:

* _Concatenation_ - a pattern like `(A B)` means that the contiguity is strict between `A` and `B`. Therefore, there can be no rows that were not mapped to `A` or `B` in between.
* _Quantifiers_ - modify the number of rows that can be mapped to the pattern variable.
  * `*` — _0_ or more rows
  * `+` — _1_ or more rows
  * `?` — _0_ or _1_ rows
  * `{ n }` — exactly _n_ rows (_n > 0_)
  * `{ n, }` — _n_ or more rows (_n ≥ 0_)
  * `{ n, m }` — between _n_ and _m_ (inclusive) rows (_0 ≤ n ≤ m, 0 < m_)
  * `{ , m }` — between _0_ and _m_ (inclusive) rows (_m > 0_)

<span class="label label-danger">Attention</span> Patterns that can potentially produce an empty match are not supported.
Examples of such patterns are `PATTERN (A*)`, `PATTERN  (A? B*)`, `PATTERN (A{0,} B{0,} C*)`, etc.

### Greedy & Reluctant Quantifiers

Each quantifier can be either _greedy_ (default behavior) or _reluctant_. Greedy quantifiers try to match
as many rows as possible while reluctant quantifiers try to match as few as possible.

In order to illustrate the difference, one can view the following example with a query where a greedy quantifier is applied to the `B` variable:

{% highlight sql %}
SELECT *
FROM Ticker
    MATCH_RECOGNIZE(
        PARTITION BY symbol
        ORDER BY rowtime
        MEASURES
            C.price AS lastPrice
        ONE ROW PER MATCH
        AFTER MATCH SKIP PAST LAST ROW
        PATTERN (A B* C)
        DEFINE
            A AS A.price > 10,
            B AS B.price < 15,
            C AS C.price > 12
    )
{% endhighlight %}

Given we have the following input:

{% highlight text %}
 symbol  tax   price          rowtime
======= ===== ======== =====================
 XYZ     1     10       2018-09-17 10:00:02
 XYZ     2     11       2018-09-17 10:00:03
 XYZ     1     12       2018-09-17 10:00:04
 XYZ     2     13       2018-09-17 10:00:05
 XYZ     1     14       2018-09-17 10:00:06
 XYZ     2     16       2018-09-17 10:00:07
{% endhighlight %}

The pattern above will produce the following output:

{% highlight text %}
 symbol   lastPrice
======== ===========
 XYZ      16
{% endhighlight %}

The same query where `B*` is modified to `B*?`, which means that `B*` should be reluctant, will produce:

{% highlight text %}
 symbol   lastPrice
======== ===========
 XYZ      13
 XYZ      16
{% endhighlight %}

The pattern variable `B` matches only to the row with price `12` instead of swallowing the rows with prices `12`, `13`, and `14`.

<span class="label label-danger">Attention</span> It is not possible to use a greedy quantifier for the last
variable of a pattern. Thus, a pattern like `(A B*)` is not allowed. This can be easily worked around by introducing an artificial state
(e.g. `C`) that has a negated condition of `B`. So you could use a query like:

{% highlight sql %}
PATTERN (A B* C)
DEFINE
    A AS condA(),
    B AS condB(),
    C AS NOT condB()
{% endhighlight %}

<span class="label label-danger">Attention</span> The optional reluctant quantifier (`A??` or `A{0,1}?`) is not supported right now.

Output Mode
-----------

The _output mode_ describes how many rows should be emitted for every found match. The SQL standard describes two modes:
- `ALL ROWS PER MATCH`
- `ONE ROW PER MATCH`.

Currently, the only supported output mode is `ONE ROW PER MATCH` that will always produce one output summary row for each found match.

The schema of the output row will be a concatenation of `[partitioning columns] + [measures columns]` in that particular order.

The following example shows the output of a query defined as:

{% highlight sql %}
SELECT *
FROM Ticker
    MATCH_RECOGNIZE(
        PARTITION BY symbol
        ORDER BY rowtime
        MEASURES
            FIRST(A.price) AS startPrice,
            LAST(A.price) AS topPrice,
            B.price AS lastPrice
        ONE ROW PER MATCH
        PATTERN (A+ B)
        DEFINE
            A AS LAST(A.price, 1) IS NULL OR A.price > LAST(A.price, 1),
            B AS B.price < LAST(A.price)
    )
{% endhighlight %}

For the following input rows:

{% highlight text %}
 symbol   tax   price          rowtime
======== ===== ======== =====================
 XYZ      1     10       2018-09-17 10:00:02
 XYZ      2     12       2018-09-17 10:00:03
 XYZ      1     13       2018-09-17 10:00:04
 XYZ      2     11       2018-09-17 10:00:05
{% endhighlight %}

The query will produce the following output:

{% highlight text %}
 symbol   startPrice   topPrice   lastPrice
======== ============ ========== ===========
 XYZ      10           13         11
{% endhighlight %}

The pattern recognition is partitioned by the `symbol` column. Even though not explicitly mentioned in the `MEASURES` clause, the partitioned column is added at the beginning of the result.

Pattern Navigation
------------------

The `DEFINE` and `MEASURES` clauses allow for navigating within the list of rows that (potentially) match a pattern.

This section discusses this navigation for declaring conditions or producing output results.

### Pattern Variable Referencing

A _pattern variable reference_ allows a set of rows mapped to a particular pattern variable in the `DEFINE` or `MEASURES` clauses to be referenced.

For example, the expression `A.price` describes a set of rows mapped so far to `A` plus the current row
if we try to match the current row to `A`. If an expression in the `DEFINE`/`MEASURES` clause requires a single row (e.g. `A.price` or `A.price > 10`),
it selects the last value belonging to the corresponding set.

If no pattern variable is specified (e.g. `SUM(price)`), an expression references the default pattern variable `*` which references all variables in the pattern.
In other words, it creates a list of all the rows mapped so far to any variable plus the current row.

#### Example

For a more thorough example, one can take a look at the following pattern and corresponding conditions:

{% highlight sql %}
PATTERN (A B+)
DEFINE
  A AS A.price > 10,
  B AS B.price > A.price AND SUM(price) < 100 AND SUM(B.price) < 80
{% endhighlight %}

The following table describes how those conditions are evaluated for each incoming event.

The table consists of the following columns:
  * `#` - the row identifier that uniquely identifies an incoming row in the lists `[A.price]`/`[B.price]`/`[price]`.
  * `price` - the price of the incoming row.
  * `[A.price]`/`[B.price]`/`[price]` - describe lists of rows which are used in the `DEFINE` clause to evaluate conditions.
  * `Classifier` - the classifier of the current row which indicates the pattern variable the row is mapped to.
  * `A.price`/`B.price`/`SUM(price)`/`SUM(B.price)` - describes the result after those expressions have been evaluated.

<table class="table table-bordered">
  <thead>
    <tr>
      <th>#</th>
      <th>price</th>
      <th>Classifier</th>
      <th>[A.price]</th>
      <th>[B.price]</th>
      <th>[price]</th>
      <th>A.price</th>
      <th>B.price</th>
      <th>SUM(price)</th>
      <th>SUM(B.price)</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>#1</td>
      <td>10</td>
      <td>-&gt; A</td>
      <td>#1</td>
      <td>-</td>
      <td>-</td>
      <td>10</td>
      <td>-</td>
      <td>-</td>
      <td>-</td>
    </tr>
    <tr>
      <td>#2</td>
      <td>15</td>
      <td>-&gt; B</td>
      <td>#1</td>
      <td>#2</td>
      <td>#1, #2</td>
      <td>10</td>
      <td>15</td>
      <td>25</td>
      <td>15</td>
    </tr>
    <tr>
      <td>#3</td>
      <td>20</td>
      <td>-&gt; B</td>
      <td>#1</td>
      <td>#2, #3</td>
      <td>#1, #2, #3</td>
      <td>10</td>
      <td>20</td>
      <td>45</td>
      <td>35</td>
    </tr>
    <tr>
      <td>#4</td>
      <td>31</td>
      <td>-&gt; B</td>
      <td>#1</td>
      <td>#2, #3, #4</td>
      <td>#1, #2, #3, #4</td>
      <td>10</td>
      <td>31</td>
      <td>76</td>
      <td>66</td>
    </tr>
    <tr>
      <td>#5</td>
      <td>35</td>
      <td></td>
      <td>#1</td>
      <td>#2, #3, #4, #5</td>
      <td>#1, #2, #3, #4, #5</td>
      <td>10</td>
      <td>35</td>
      <td>111</td>
      <td>101</td>
    </tr>
  </tbody>
</table>

As can be seen in the table, the first row is mapped to pattern variable `A` and subsequent rows are mapped to pattern variable `B`. However, the last row does not fulfill the `B` condition because the sum over all mapped rows `SUM(price)` and the sum over all rows in `B` exceed the specified thresholds.

<span class="label label-danger">Attention</span> Please note that aggregations such as `SUM` are not supported yet. They are only used for explanation here.

### Logical Offsets

_Logical offsets_ enable navigation within the events that were mapped to a particular pattern variable. This can be expressed
with two corresponding functions:

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Offset functions</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  <tbody>
  <tr>
    <td>
      {% highlight text %}
LAST(variable.field, n)
{% endhighlight %}
    </td>
    <td>
      <p>Returns the value of the field from the event that was mapped to the <i>n</i>-th <i>last</i> element of the variable. The counting starts at the last element mapped.</p>
    </td>
  </tr>
  <tr>
    <td>
      {% highlight text %}
FIRST(variable.field, n)
{% endhighlight %}
    </td>
    <td>
      <p>Returns the value of the field from the event that was mapped to the <i>n</i>-th element of the variable. The counting starts at the first element mapped.</p>
    </td>
  </tr>
  </tbody>
</table>

#### Examples

For a more thorough example, one can take a look at the following pattern and corresponding conditions:

{% highlight sql %}
PATTERN (A B+)
DEFINE
  A AS A.price > 10,
  B AS (LAST(B.price, 1) IS NULL OR B.price > LAST(B.price, 1)) AND
       (LAST(B.price, 2) IS NULL OR B.price > 2 * LAST(B.price, 2))
{% endhighlight %}

The following table describes how those conditions are evaluated for each incoming event.

The table consists of the following columns:
  * `price` - the price of the incoming row.
  * `Classifier` - the classifier of the current row which indicates the pattern variable the row is mapped to.
  * `LAST(B.price, 1)`/`LAST(B.price, 2)` - describes the result after those expressions have been evaluated.

<table class="table table-bordered">
  <thead>
    <tr>
        <th style="white-space:nowrap">price</th>
        <th style="white-space:nowrap">Classifier</th>
        <th style="white-space:nowrap">LAST(B.price, 1)</th>
        <th style="white-space:nowrap">LAST(B.price, 2)</th>
        <th>Comment</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>10</td>
      <td>-&gt; A</td>
      <td></td>
      <td></td>
      <td></td>
    </tr>
    <tr>
      <td>15</td>
      <td>-&gt; B</td>
      <td>null</td>
      <td>null</td>
      <td>Notice that <code>LAST(A.price, 1)</code> is null because there is still nothing mapped to <code>B</code>.</td>
    </tr>
    <tr>
      <td>20</td>
      <td>-&gt; B</td>
      <td>15</td>
      <td>null</td>
      <td></td>
    </tr>
    <tr>
      <td>31</td>
      <td>-&gt; B</td>
      <td>20</td>
      <td>15</td>
      <td></td>
    </tr>
    <tr>
      <td>35</td>
      <td></td>
      <td>31</td>
      <td>20</td>
      <td>Not mapped because <code>35 &lt; 2 * 20</code>.</td>
    </tr>
  </tbody>
</table>

It might also make sense to use the default pattern variable with logical offsets.

In this case, an offset considers all the rows mapped so far:

{% highlight sql %}
PATTERN (A B? C)
DEFINE
  B AS B.price < 20,
  C AS LAST(price, 1) < C.price
{% endhighlight %}

<table class="table table-bordered">
  <thead>
    <tr>
        <th style="white-space:nowrap">price</th>
        <th style="white-space:nowrap">Classifier</th>
        <th style="white-space:nowrap">LAST(price, 1)</th>
        <th>Comment</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>10</td>
      <td>-&gt; A</td>
      <td></td>
      <td></td>
    </tr>
    <tr>
      <td>15</td>
      <td>-&gt; B</td>
      <td></td>
      <td></td>
    </tr>
    <tr>
      <td>20</td>
      <td>-&gt; C</td>
      <td>15</td>
      <td><code>LAST(price, 1)</code> is evaluated as the price of the row mapped to the <code>B</code> variable.</td>
    </tr>
  </tbody>
</table>

If the second row did not map to the `B` variable, we would have the following results:

<table class="table table-bordered">
  <thead>
    <tr>
        <th style="white-space:nowrap">price</th>
        <th style="white-space:nowrap">Classifier</th>
        <th style="white-space:nowrap">LAST(price, 1)</th>
        <th>Comment</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>10</td>
      <td>-&gt; A</td>
      <td></td>
      <td></td>
    </tr>
    <tr>
      <td>20</td>
      <td>-&gt; C</td>
      <td>10</td>
      <td><code>LAST(price, 1)</code> is evaluated as the price of the row mapped to the <code>A</code> variable.</td>
    </tr>
  </tbody>
</table>

It is also possible to use multiple pattern variable references in the first argument of the `FIRST/LAST` functions. This way, one can write an expression that accesses multiple columns.
However, all of them must use the same pattern variable. In other words, the value of the `LAST`/`FIRST` function must be computed in a single row.

Thus, it is possible to use `LAST(A.price * A.tax)`, but an expression like `LAST(A.price * B.tax)` is not allowed.

After Match Strategy
--------------------

The `AFTER MATCH SKIP` clause specifies where to start a new matching procedure after a complete match was found.

There are four different strategies:
* `SKIP PAST LAST ROW` - resumes the pattern matching at the next row after the last row of the current match.
* `SKIP TO NEXT ROW` - continues searching for a new match starting at the next row after the starting row of the match.
* `SKIP TO LAST variable` - resumes the pattern matching at the last row that is mapped to the specified pattern variable.
* `SKIP TO FIRST variable` - resumes the pattern matching at the first row that is mapped to the specified pattern variable.

This is also a way to specify how many matches a single event can belong to. For example, with the `SKIP PAST LAST ROW` strategy every event can belong to at most one match.

#### Examples

In order to better understand the differences between those strategies one can take a look at the following example.

For the following input rows:

{% highlight text %}
 symbol   tax   price         rowtime
======== ===== ======= =====================
 XYZ      1     7       2018-09-17 10:00:01
 XYZ      2     9       2018-09-17 10:00:02
 XYZ      1     10      2018-09-17 10:00:03
 XYZ      2     5       2018-09-17 10:00:04
 XYZ      2     17      2018-09-17 10:00:05
 XYZ      2     14      2018-09-17 10:00:06
{% endhighlight %}

We evaluate the following query with different strategies:

{% highlight sql %}
SELECT *
FROM Ticker
    MATCH_RECOGNIZE(
        PARTITION BY symbol
        ORDER BY rowtime
        MEASURES
            SUM(A.price) AS sumPrice,
            FIRST(rowtime) AS startTime,
            LAST(rowtime) AS endTime
        ONE ROW PER MATCH
        [AFTER MATCH STRATEGY]
        PATTERN (A+ C)
        DEFINE
            A AS SUM(A.price) < 30
    )
{% endhighlight %}

The query returns the sum of the prices of all rows mapped to `A` and the first and last timestamp of the overall match.

<span class="label label-danger">Attention</span> Please note that aggregations such as `SUM` are not supported yet. They are only used for explanation here.

The query will produce different results based on which `AFTER MATCH` strategy was used:

##### `AFTER MATCH SKIP PAST LAST ROW`

{% highlight text %}
 symbol   sumPrice        startTime              endTime
======== ========== ===================== =====================
 XYZ      26         2018-09-17 10:00:01   2018-09-17 10:00:04
 XYZ      17         2018-09-17 10:00:05   2018-09-17 10:00:06
{% endhighlight %}

The first result matched against the rows #1, #2, #3, #4.

The second result matched against the rows #5, #6.

##### `AFTER MATCH SKIP TO NEXT ROW`

{% highlight text %}
 symbol   sumPrice        startTime              endTime
======== ========== ===================== =====================
 XYZ      26         2018-09-17 10:00:01   2018-09-17 10:00:04
 XYZ      24         2018-09-17 10:00:02   2018-09-17 10:00:05
 XYZ      15         2018-09-17 10:00:03   2018-09-17 10:00:05
 XYZ      22         2018-09-17 10:00:04   2018-09-17 10:00:06
 XYZ      17         2018-09-17 10:00:05   2018-09-17 10:00:06
{% endhighlight %}

Again, the first result matched against the rows #1, #2, #3, #4.

Compared to the previous strategy, the next match includes row #2 again for the next matching. Therefore, the second result matched against the rows #2, #3, #4, #5.

The third result matched against the rows #3, #4, #5.

The forth result matched against the rows #4, #5, #6.

The last result matched against the rows #5, #6.

##### `AFTER MATCH SKIP TO LAST A`

{% highlight text %}
 symbol   sumPrice        startTime              endTime
======== ========== ===================== =====================
 XYZ      26         2018-09-17 10:00:01   2018-09-17 10:00:04
 XYZ      15         2018-09-17 10:00:03   2018-09-17 10:00:05
 XYZ      22         2018-09-17 10:00:04   2018-09-17 10:00:06
 XYZ      17         2018-09-17 10:00:05   2018-09-17 10:00:06
{% endhighlight %}

Again, the first result matched against the rows #1, #2, #3, #4.

Compared to the previous strategy, the next match includes only row #3 (mapped to `A`) again for the next matching. Therefore, the second result matched against the rows #3, #4, #5.

The third result matched against the rows #4, #5, #6.

The last result matched against the rows #5, #6.

##### `AFTER MATCH SKIP TO FIRST A`

This combination will produce a runtime exception because one would always try to start a new match where the
last one started. This would produce an infinite loop and, thus, is prohibited.

One has to keep in mind that in case of the `SKIP TO FIRST/LAST variable` strategy it might be possible that there are no rows mapped to that
variable (e.g. for pattern `A*`). In such cases, a runtime exception will be thrown as the standard requires a valid row to continue the
matching.


### Controlling Memory Consumption

Memory consumption is an important consideration when writing `MATCH_RECOGNIZE` queries, as the space of potential matches is built in a breadth-first-like manner.
Having that in mind, one must make sure that the pattern can finish. Preferably with a reasonable number of rows mapped to the match as they have to fit into memory.

For example, the pattern must not have a quantifier without an upper limit that accepts every single row. Such a pattern could look like this:

{% highlight sql %}
PATTERN (A B+ C)
DEFINE
  A as A.price > 10,
  C as C.price > 20
{% endhighlight %}

The query will map every incoming row to the `B` variable and thus will never finish. This query could be fixed, e.g., by negating the condition for `C`:

{% highlight sql %}
PATTERN (A B+ C)
DEFINE
  A as A.price > 10,
  B as B.price <= 20,
  C as C.price > 20
{% endhighlight %}

Or by using the [reluctant quantifier](#greedy--reluctant-quantifiers):

{% highlight sql %}
PATTERN (A B+? C)
DEFINE
  A as A.price > 10,
  C as C.price > 20
{% endhighlight %}

<span class="label label-danger">Attention</span> Please note that the `MATCH_RECOGNIZE` clause does not use a configured [state retention time](query_configuration.html#idle-state-retention-time). As of now, there is also no possibility to define a time restriction on the pattern to finish because there is no such possibility in the SQL standard. The community is in the process of designing a proper syntax for that
feature right now.

Known Limitations
-----------------

Flink's implementation of the `MATCH_RECOGNIZE` clause is an ongoing effort, and some features of the SQL standard are not yet supported.

Unsupported features include:
* Pattern expressions:
  * Pattern groups - this means that e.g. quantifiers can not be applied to a subsequence of the pattern. Thus, `(A (B C)+)` is not a valid pattern.
  * Alterations - patterns like `PATTERN((A B | C D) E)`, which means that either a subsequence `A B` or `C D` has to be found before looking for the `E` row.
  * `PERMUTE` operator - which is equivalent to all permutations of variables that it was applied to e.g. `PATTERN (PERMUTE (A, B, C))` = `PATTERN (A B C | A C B | B A C | B C A | C A B | C B A)`.
  * Anchors - `^, $`, which denote beginning/end of a partition, those do not make sense in the streaming context and will not be supported.
  * Exclusion - `PATTERN ({- A -} B)` meaning that `A` will be looked for but will not participate in the output. This works only for the `ALL ROWS PER MATCH` mode.
  * Reluctant optional quantifier - `PATTERN A??` only the greedy optional quantifier is supported.
* `ALL ROWS PER MATCH` output mode - which produces an output row for every row that participated in the creation of a found match. This also means:
  * that the only supported semantic for the `MEASURES` clause is `FINAL`
  * `CLASSIFIER` function, which returns the pattern variable that a row was mapped to, is not yet supported.
* `SUBSET` - which allows creating logical groups of pattern variables and using those groups in the `DEFINE` and `MEASURES` clauses.
* Physical offsets - `PREV/NEXT`, which indexes all events seen rather than only those that were mapped to a pattern variable(as in [logical offsets](#logical-offsets) case).
* Extracting time attributes - there is currently no possibility to get a time attribute for subsequent time-based operations.
* Aggregates - one cannot use aggregates in `MEASURES` nor `DEFINE` clauses.
* User defined functions cannot be used within `MATCH_RECOGNIZE`.
* `MATCH_RECOGNIZE` is supported only for SQL. There is no equivalent in the Table API.
