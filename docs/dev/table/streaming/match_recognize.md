---
title: 'Detecting patterns'
nav-parent_id: streaming_tableapi
nav-title: 'Detecting patterns'
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

It is a common use-case to search for a set of event patterns, especially in case of data streams. Apache Flink
comes with [CEP library]({{ site.baseurl }}/dev/libs/cep.html) which allows pattern detection in event streams. On the other hand Flink's
SQL API provides a relational way to express queries that comes with multiple functions and
optimizations that can be used out of the box. In December 2016, ISO released a new version of the
international SQL standard ([ISO/IEC 9075:2016](https://standards.iso.org/ittf/PubliclyAvailableStandards/c065143_ISO_IEC_TR_19075-5_2016.zip))
including the Row Pattern Recognition for complex event processing, which allowes to consolidate those two APIs using `MATCH_RECOGNIZE` clause.

`MATCH_RECOGNIZE` enables you to do the following tasks:
* Logically partition and order the data that is used with `PARTITION BY` and `ORDER BY` clauses.
* Define patterns of rows to seek using the `PATTERN` clause. These patterns use a powerful and expressive regular expression syntax.
* Specify logical conditions required to map a row to a row pattern variable in the `DEFINE` clause.
* Define measures, which are expressions usable in other parts of the SQL query, in the `MEASURES` clause.

Every `MATCH_RECOGNIZE` query consists of the following clauses:

* [PARTITION BY](#partitioning) - defines logical partitioning of the stream, similar to `GROUP BY` operation.
* [ORDER BY](#order-of-events) - specifies how the incoming events should be ordered, this is essential as patterns depend on order.
* [MEASURES](#define--measures) - defines output of the clause, similar to `SELECT` clause
* [ONE ROW PER MATCH](#output-mode) - output mode which defines how many rows per match should be produced
* [AFTER MATCH SKIP](#after-match-skip) - allows to specify where the next match should start, this is also a way to control how many distinct matches a single event can belong to
* [PATTERN](#defining-pattern) - allows constructing patterns that will be searched for using a regular expression like syntax
* [DEFINE](#define--measures) - this section defines conditions on events that should be met in order to be qualified to the corresponding pattern variable


This clause can only be applied to an [append](dynamic_tables.html#update-and-append-queries) table and it always produces
an append table as well.

* This will be replaced by the TOC
{:toc}

Example query
-------------

Having a table `TICKER`, describing prices of stocks in a particular moment of time, each row represents an updated characteristic of a ticker.
The table has a following schema:

{% highlight text %}
Ticker
     |-- symbol: Long                             # symbol of the stock
     |-- price: Long                              # price of the stock
     |-- tax: Long                                # tax liability of the stock
     |-- rowTime: TimeIndicatorTypeInfo(rowtime)  # point in time when change to those values happened
{% endhighlight %}

The incoming data for a single ticker could look following:

{% highlight text %}
SYMBOL         ROWTIME         PRICE   TAX
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

For example to find periods of a constantly decreasing price of a single ticker one could write a query like this:

{% highlight sql %}
SELECT *
FROM Ticker
MATCH_RECOGNIZE (
    PARTITION BY symbol
    ORDER BY rowtime
    MEASURES
       STRT_ROW.rowtime AS start_tstamp,
       LAST(PRICE_DOWN.rowtime) AS bottom_tstamp,
       LAST(PRICE_UP.rowtime) AS end_tstamp
    ONE ROW PER MATCH
    AFTER MATCH SKIP TO LAST UP
    PATTERN (STRT_ROW PRICE_DOWN+ PRICE_UP)
    DEFINE
       PRICE_DOWN AS PRICE_DOWN.price < LAST(PRICE_DOWN.price, 1) OR
               (LAST(PRICE_DOWN.price, 1) IS NULL AND PRICE_DOWN.price < STRT_ROW.price))
       PRICE_UP AS PRICE_UP.price > LAST(PRICE_DOWN.price, 1)
    ) MR;
{% endhighlight %}

which will produce a summary row for each found period in which the price was constantly decreasing.

{% highlight text %}
SYMBOL          START_TST          BOTTOM_TS         END_TSTAM
=========  ==================  ==================  ==================
ACME       01-APR-11 10:00:04  01-APR-11 10:00:07  01-APR-11 10:00:08
{% endhighlight %}

The resulting row says that the query found a period of a decreasing price for a ticker that started at `01-APR-11 10:00:04`,
achieved the lowest value at `01-APR-11 10:00:07` and the price increased at `01-APR-11 10:00:08`

Installation guide
------------------

Pattern recognition feature uses the Apache Flink's CEP library internally. In order to be able to use this clause one has to add
the library as a dependency. Either by adding it to your uber-jar or by adding a dependency on:

{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-cep{{ site.scala_version_suffix }}</artifactId>
  <version>{{ site.version }}</version>
</dependency>
{% endhighlight %}

or by adding it to the cluster classpath (see [here]({{ site.baseurl}}/dev/linking.html)). If you want to use
the `MATCH_RECOGNIZE` clause from [sql-client]({{ site.baseurl}}/dev/table/sqlClient.html) you don't have to do anything as all the dependencies are included by default.

Partitioning
------------
It is possible to look for patterns in a partitioned data, e.g. trends for a single ticker. This can be expressed using the `PARTITION BY` clause. It is similar to using the `GROUP BY` for aggregations.

<span class="label label-danger">Attention</span> It is highly advised to partition the incoming data because otherwise the `MATCH_RECOGNIZE` will be translated
into a non-parallel operator to ensure global ordering.

Order of events
---------------

Apache Flink allows searching for patterns based on time, either [processing-time or event-time](time_attributes.html). This assumption
is very important, because it allows sorting events before they are passed to pattern state machine. This ensures
that the produced output will be correct in regards not to the order the rows arrived, but in which the event corresponding to the row really happened.

As a consequence one has to provide a time attribute with ascending ordering as the first argument to `ORDER BY` clause.
That means for the example `TICKER` table, a definition like `ORDER BY rowtime ASC, price DESC` is valid, but `ORDER BY price, rowtime` or `ORDER BY rowtime DESC, price ASC` is not.

Define & Measures
-----------------

`DEFINE` and `MEASURES` keywords have similar functions as `WHERE` and `SELECT` clauses in a simple SQL query.

Using the `MEASURES` clause you can define what will be included in the output. What exactly will be produced depends also
on the [output mode](#output-mode) setting.

On the other hand `DEFINE` allows to specify conditions that rows have to fulfill in order to be classified to corresponding [pattern variable](#defining-pattern).
If a condition is not defined for a pattern variable, a default condition will be used, which evaluates to `true` for every row.

For a more thorough explanation on expressions that you can use in those clauses please have a look at [event stream navigation](#event-stream-navigation).

Defining pattern
----------------

`MATCH_RECOGNIZE` clause allows user to search for patterns in event streams using a powerful and expressive language
that is somewhat similar to widespread regular expression syntax. Every pattern is constructed from building blocks called
pattern variables, to whom operators (quantifiers and other modifiers) can be applied. The whole pattern must be enclosed in
brackets. Example pattern:

{% highlight sql %}
PATTERN (A B+ C*? D)
{% endhighlight %}

One may use the following operators:

* Concatenation - a pattern like (A B) means that between the A B the contiguity is strict. This means there can be no rows that were not mapped to A or B in between
* Quantifiers - modifies the number of rows that can be mapped to pattern variable
  * `*` — 0 or more rows
  * `+` — 1 or more rows
  * `?` — 0 or 1 rows
  * `{ n }` — exactly n rows (n > 0)
  * `{ n, }` — n or more rows (n ≥ 0)
  * `{ n, m }` — between n and m (inclusive) rows (0 ≤ n ≤ m, 0 < m)
  * `{ , m }` — between 0 and m (inclusive) rows (m > 0)

<span class="label label-danger">Attention</span> Patterns that can potentially produce empty match are not supported.
Examples of such patterns are: `PATTERN (A*)`, `PATTERN  (A? B*)`, `PATTERN (A{0,} B{0,} C*)` etc.

### Greedy & reluctant quantifiers

Each quantifier can be either greedy (true by default) or reluctant. The difference is that greedy quantifiers try to match
as many rows as possible, while reluctant as few as possible. To better illustrate the difference one can analyze following example:

Query with greedy quantifier applied to `B` variable:
{% highlight sql %}
SELECT *
FROM Ticker
    MATCH_RECOGNIZE(
        PARTITION BY symbol
        ORDER BY rowtime
        MEASURES
            C.price as lastPrice
        PATTERN (A B* C)
        ONE ROW PER MATCH
        AFTER MATCH SKIP PAST LAST ROW
        DEFINE
            A as A.price > 10
            B as B.price < 15
            C as B.price > 12
    )
{% endhighlight %}

For input:

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

Will produce output:

{% highlight text %}
 symbol   lastPrice
======== ===========
 XYZ      16
{% endhighlight %}

but the same query with just the `B*` modified to `B*?`, which means it should be reluctant quantifier, will produce:

{% highlight text %}
 symbol   lastPrice
======== ===========
 XYZ      13
{% endhighlight %}

<span class="label label-danger">Attention</span> It is not possible to use a greedy quantifier for the last
variable for a pattern, thus pattern like `(A B*)` is not allowed. This can be easily worked around by introducing an artificial state
e.g. `C` that will have a negated condition of `B`. So you could use a query like:

{% highlight sql %}
PATTERN (A B* C)
DEFINE
    A as condA()
    B as condB()
    C as NOT condB()
{% endhighlight %}

<span class="label label-danger">Attention</span> The optional reluctant quantifier (`A??` or `A{0,1}?`) is not supported right now.

Output mode
-----------

The output mode describes how many rows should be emitted for every found match. The SQL standard describes two modes: `ALL ROWS PER MATCH` and
`ONE ROW PER MATCH`.

Currently the only supported output mode is `ONE ROW PER MATCH` that will always produce one output summary row per each found match.
The schema of the output row will be a union of `{partitioning columns} + {measures columns}` in that particular order.

Example:

Query:
{% highlight sql %}
SELECT *
FROM Ticker
    MATCH_RECOGNIZE(
        PARTITION BY symbol
        ORDER BY rowtime
        MEASURES
            FIRST(A.price) as startPrice
            LAST(A.price) as topPrice
            B.price as lastPrice
        PATTERN (A+ B)
        ONE ROW PER MATCH
        DEFINE
            A as A.price > LAST(A.price, 1) OR LAST(A.price, 1) IS NULL,
            B as B.price < LAST(A.price)
    )
{% endhighlight %}

for input:

{% highlight text %}
 symbol   tax   price.         rowtime
======== ===== ======== =====================
 XYZ      1     10       2018-09-17 10:00:02
 XYZ      2     12       2018-09-17 10:00:03
 XYZ      1     13       2018-09-17 10:00:04
 XYZ      2     11       2018-09-17 10:00:05
{% endhighlight %}

will produce:

{% highlight text %}
 symbol   startPrice   topPrice   lastPrice
======== ============ ========== ===========
 XYZ      10           13         11
{% endhighlight %}

Event stream navigation
------------------

### Pattern variable reference
Pattern variable reference allows referencing a set of rows mapped to a particular pattern variable in
`DEFINE` or `MEASURE` clauses. For example, expression `A.price` describes a set of rows mapped so far to `A` plus the current row
if we try to match current row to `A`. If an expression in the `DEFINE`/`MEASURES` requires a single row e.g. `A.price > 10`, `A.price`
it selects the last value belonging to the corresponding set.

If no pattern variable is specified e.g. `SUM(price)` it references the default pattern variable `*` which references all variables in the pattern.
In other words it creates a set of all the rows mapped so far to any variable plus the current row.

For a more thorough example one can analyze the following pattern and corresponding conditions:

{% highlight sql %}
PATTERN (A B+)
DEFINE
  A as A.price > 10,
  B as B.price > A.price AND SUM(price) < 100 AND SUM(B.price) < 80
{% endhighlight %}

The following table describes how those conditions are evaluated for each incoming event. That table consists of following columns:
  * `no` - a row identifier, to uniquely reference in `{A.price}/{B.price}/{price}` columns
  * `{A.price}/{B.price}/{price}` - describes set of rows which are used in the `DEFINE` clause to evaluate conditions.
  * `CLASSIFIER` - is a classifier of the current row, which tells which variable was that row mapped to
  * `A.price/B.price/SUM(price)/SUM(B.price)` -  describes the result of those expression

<table class="table table-bordered">
  <thead>
    <tr>
      <th>no</th>
      <th>price</th>
      <th>CLASSIFIER</th>
      <th>{A.price}</th>
      <th>{B.price}</th>
      <th>{price}</th>
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
      <td>#1,#2</td>
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
      <td>#2,#3</td>
      <td>#1,#2,#3</td>
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
      <td>#2,#3,#4</td>
      <td>#1,#2,#3,#4</td>
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
      <td>#2,#3,#4,#5</td>
      <td>#1,#2,#3,#4,#5</td>
      <td>10</td>
      <td>35</td>
      <td>111</td>
      <td>101</td>
    </tr>
  </tbody>
</table>

### Pattern variable indexing

Apache Flink allows navigating within events that were mapped to a particular pattern variable with so called logical offsets. This can be expressed
with two corresponding functions:

<div data-lang="SQL" markdown="1">
  <table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left" style="width: 40%">Comparison functions</th>
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
        <p>Returns the value of the field from the event that was mapped to the n-th element of the variable, counting from the last element mapped.</p>
      </td>
    </tr>
    <tr>
      <td>
        {% highlight text %}
FIRST(variable.field, n)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the value of the field from the event that was mapped to the n-th element of the variable, counting from the first element mapped.</p>
      </td>
    </tr>
    </tbody>
  </table>

Example query
{% highlight sql %}
PATTERN (A B+)
DEFINE
  A as A.price > 10,
  B as (B.price > LAST(B.price, 1) OR LAST(B.price, 1) IS NULL) AND
       (B.price > 2 * LAST(B.price, 2) OR LAST(B.price, 2) IS NULL)
{% endhighlight %}

will be evaluated as follows:

<table class="table table-bordered">
  <thead>
    <tr>
        <th style="white-space:nowrap">price</th>
        <th style="white-space:nowrap">CLASSIFIER</th>
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
      <td>Notice that LAST(A.price, 1) is null, because there is still nothing mapped to B</td>
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
      <td>Not mapped because 35 &lt; 2 * 20</td>
    </tr>
  </tbody>
</table>

It might also make sense to use the default pattern variable with logical offsets, which allows indexing within all the rows mapped so far:

{% highlight sql %}
PATTERN (A B? C)
DEFINE
  B as B.price < 20,
  C as LAST(price, 1) < C.price
{% endhighlight %}

<table class="table table-bordered">
  <thead>
    <tr>
        <th style="white-space:nowrap">price</th>
        <th style="white-space:nowrap">CLASSIFIER</th>
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
      <td>LAST(price, 1) was evaluated as the price of the row mapped to the B variable</td>
    </tr>
  </tbody>
</table>

but if the second row did not map to `B` variable then we would have the following results:

<table class="table table-bordered">
  <thead>
    <tr>
        <th style="white-space:nowrap">price</th>
        <th style="white-space:nowrap">CLASSIFIER</th>
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
      <td>LAST(price, 1) was evaluated as the price of the row mapped to the A variable.</td>
    </tr>
  </tbody>
</table>

It is also possible to use multiple pattern variable references but all of them must reference the same variable.
This way one can write an expression on multiple columns.
In other words the value of `LAST`/`FIRST` function must be computed in a single row.

Thus it is possible to use `LAST(A.price * A.tax)`, but an expression like `LAST(A.price * B.tax)` is not allowed.

After match skip
----------------

This clause specifies where to start a new matching procedure after a complete match was found.

There are five different strategies:
* `SKIP TO NEXT ROW` - continues searching for a new match starting at the next element to the starting element of a match
* `SKIP PAST LAST ROW` - resumes the pattern matching at the next row after the last row of the current match.
* `SKIP TO FIRST variable` - resumes the pattern matching at the first row that is mapped to the pattern variable
* `SKIP TO LAST variable` - resumes the pattern matching at the last row that is mapped to the pattern variable

This is also a way to specify how many matches a single event can belong to, e.g. with the `SKIP PAST LAST ROW` every event can belong to at most one match.

To better understand the differences between those strategies one can analyze the following example:

Query:

{% highlight sql %}
SELECT *
FROM Ticker
    MATCH_RECOGNIZE(
        PARTITION BY symbol
        ORDER BY rowtime
        MEASURES
            SUM(A.price) as sumPrice,
            FIRST(A.rowtime) as startTime,
            LAST(A.rowtime) as endTime
        PATTERN (A+ C)
        ONE ROW PER MATCH
        [AFTER MATCH STRATEGY]
        DEFINE
            A as SUM(A.price) < 30
    )
{% endhighlight %}

For an input:

{% highlight text %}
 symbol   tax   price         rowtime
======== ===== ======= =====================
 XYZ      1     7       2018-09-17 10:00:01
 XYZ      2     9       2018-09-17 10:00:02
 XYZ      1     10      2018-09-17 10:00:03
 XYZ      2     17      2018-09-17 10:00:04
 XYZ      2     14      2018-09-17 10:00:05
{% endhighlight %}

Will produce results based on what `AFTER MATCH STRATEGY` was used:

* AFTER MATCH SKIP PAST LAST ROW

{% highlight text %}
 symbol   sumPrice        startTime              endTime
======== ========== ===================== =====================
 XYZ      26         2018-09-17 10:00:01   2018-09-17 10:00:03
 XYZ      17         2018-09-17 10:00:04   2018-09-17 10:00:04
{% endhighlight %}

* AFTER MATCH SKIP TO LAST A

{% highlight text %}
 symbol   sumPrice        startTime              endTime
======== ========== ===================== =====================
 XYZ      26         2018-09-17 10:00:01   2018-09-17 10:00:03
 XYZ      27         2018-09-17 10:00:03   2018-09-17 10:00:04
 XYZ      17         2018-09-17 10:00:04   2018-09-17 10:00:04
{% endhighlight %}

* AFTER MATCH SKIP TO NEXT ROW

{% highlight text %}
 symbol   sumPrice        startTime              endTime
======== ========== ===================== =====================
 XYZ      26         2018-09-17 10:00:01   2018-09-17 10:00:03
 XYZ      19         2018-09-17 10:00:02   2018-09-17 10:00:03
 XYZ      27         2018-09-17 10:00:03   2018-09-17 10:00:04
 XYZ      17         2018-09-17 10:00:04   2018-09-17 10:00:04
{% endhighlight %}

* AFTER MATCH SKIP TO FIRST A

This combination will produce a runtime exception, because one would always try to start a new match where the
last one started. This would produce an infinite loop, thus it is prohibited.

One has to have in mind that in case of the `SKIP TO FIRST/LAST variable` it might be possible that there are no rows mapped to that
variable.(e.g. For pattern `A*`). In such cases a RuntimeException will be thrown as the Standard requires a valid row to continue
matching.

Known limitations
-----------------

`MATCH_RECOGNIZE` clause is still an ongoing effort and therefore some SQL Standard features are not supported yet.
The list of such features includes:
* pattern expressions
  * pattern groups - this means that e.g. quantifiers can not be applied to a subsequence of the pattern, thus `(A (B C)+)` is not a valid pattern
  * alterations - patterns like `PATTERN((A B | C D) E)`, which means that either a subsequence `A B` or `C D` has to be found before looking for the `E` row
  * `PERMUTE` operator - which is equivalent to all permutations of variables that it was applied to e.g. `PATTERN (PERMUTE (A, B, C))` = `PATTERN (A B C | A C B | B A C | B C A | C A B | C B A)`
  * anchors - `^, $`, which denote beginning/end of a partition, those do not make sense in the streaming context and will not be supported
  * exclusion - `PATTERN ({- A -} B)` meaning that `A` will be looked for, but will not participate in the output, this works only for the `ALL ROWS PER MATCH` mode
  * reluctant optional quantifier - `PATTERN A??` only the greedy optional quantifier is supported
* `ALL ROWS PER MATCH` output mode, which produces an output row for every row that participated in the creation of a found match. This also means:
  * that the only supported semantic for the `MEASURES` clause is `FINAL`
  * `CLASSIFIER` function, which returns the pattern variable that a row was mapped to, is not yet supported
* `SUBSET` - which allows creating logical groups of pattern variables and using those groups in the `DEFINE` and `MEASURES` clauses
* physical offsets - `PREV/NEXT`, which indexes all events seen rather than only those that were mapped to a pattern variable(as in [logical offsets](#pattern-variable-indexing) case)
* there is no support of aggregates yet, one cannot use aggregates in `MEASURES` nor `DEFINE` clauses
* user defined functions cannot be used within `MATCH_RECOGNIZE`
* `MATCH_RECOGNIZE` is supported only for SQL, there is no equivalent in the table API


### Controlling memory consumption

Memory consumption is an important aspect when writing `MATCH_RECOGNIZE` queries, as the space of potential matches is built in a breadth first like manner.
Having that in mind one must make sure that the pattern can finish(preferably with a reasonable number of rows mapped to the match as they have to fit into memory) e.g.
it does not have a quantifier without an upper limit that accepts every single row. Such pattern could look like this:

{% highlight sql %}
PATTERN (A B+ C)
DEFINE
  A as A.price > 10,
  C as C.price > 20
{% endhighlight %}

which will map every incoming row to the `B` variable and thus will never finish. That query could be fixed e.g by negating the condition for `C`:

{% highlight sql %}
PATTERN (A B+ C)
DEFINE
  A as A.price > 10,
  B as B.price <= 20,
  C as C.price > 20
{% endhighlight %}

or using the [reluctant quantifier](#greedy--reluctant-quantifiers):

{% highlight sql %}
PATTERN (A B+? C)
DEFINE
  A as A.price > 10,
  C as C.price > 20
{% endhighlight %}

One has to be aware that `MATCH_RECOGNIZE` clause does not use [state retention time](query_configuration.html#idle-state-retention-time). There is also no possibility to
define a time restriction on the pattern to finish as of now, because there is no such possibility in SQL standard. The Community is in the process of designing a proper syntax for that
feature right now.
