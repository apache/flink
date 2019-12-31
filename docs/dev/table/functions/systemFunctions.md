---
title: "System (Built-in) Functions"
nav-parent_id: table_functions
nav-pos: 31
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

Flink Table API & SQL provides users with a set of built-in functions for data transformations. This page gives a brief overview of them.
If a function that you need is not supported yet, you can implement a <a href="udfs.html">user-defined function</a>.
If you think that the function is general enough, please <a href="https://issues.apache.org/jira/secure/CreateIssue!default.jspa">open a Jira issue</a> for it with a detailed description.

* This will be replaced by the TOC
{:toc}

Scalar Functions
----------------

The scalar functions take zero, one or more values as the input and return a single value as the result.

### Comparison Functions

<div class="codetabs" markdown="1">

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
value1 = value2
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if <i>value1</i> is equal to <i>value2</i>; returns UNKNOWN if <i>value1</i> or <i>value2</i> is NULL.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
value1 <> value2
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if <i>value1</i> is not equal to <i>value2</i>; returns UNKNOWN if <i>value1</i> or <i>value2</i> is NULL. </p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
value1 > value2
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if <i>value1</i> is greater than <i>value2</i>; returns UNKNOWN if <i>value1</i> or <i>value2</i> is NULL.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
value1 >= value2
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if <i>value1</i> is greater than or equal to <i>value2</i>; returns UNKNOWN if <i>value1</i> or <i>value2</i> is NULL.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
value1 < value2
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if <i>value1</i> is less than <i>value2</i>; returns UNKNOWN if <i>value1</i> or <i>value2</i> is NULL.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
value1 <= value2
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if <i>value1</i> is less than or equal to <i>value2</i>; returns UNKNOWN if <i>value1</i> or <i>value2</i> is NULL.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
value IS NULL
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if <i>value</i> is NULL.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
value IS NOT NULL
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if <i>value</i> is not NULL.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
value1 IS DISTINCT FROM value2
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if two values are not equal. NULL values are treated as identical here.</p>
        <p>E.g., <code>1 IS DISTINCT FROM NULL</code> returns TRUE;
        <code>NULL IS DISTINCT FROM NULL</code> returns FALSE.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
value1 IS NOT DISTINCT FROM value2
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if two values are equal. NULL values are treated as identical here.</p>
        <p>E.g., <code>1 IS NOT DISTINCT FROM NULL</code> returns FALSE;
        <code>NULL IS NOT DISTINCT FROM NULL</code> returns TRUE.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
value1 BETWEEN [ ASYMMETRIC | SYMMETRIC ] value2 AND value3
{% endhighlight %}
      </td>
      <td>
        <p>By default (or with the ASYMMETRIC keyword), returns TRUE if <i>value1</i> is greater than or equal to <i>value2</i> and less than or equal to <i>value3</i>.
          With the SYMMETRIC keyword, returns TRUE if <i>value1</i> is inclusively between <i>value2</i> and <i>value3</i>. 
          When either <i>value2</i> or <i>value3</i> is NULL, returns FALSE or UNKNOWN.</p>
          <p>E.g., <code>12 BETWEEN 15 AND 12</code> returns FALSE;
          <code>12 BETWEEN SYMMETRIC 15 AND 12</code> returns TRUE;
          <code>12 BETWEEN 10 AND NULL</code> returns UNKNOWN;
          <code>12 BETWEEN NULL AND 10</code> returns FALSE;
          <code>12 BETWEEN SYMMETRIC NULL AND 12</code> returns UNKNOWN.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
value1 NOT BETWEEN [ ASYMMETRIC | SYMMETRIC ] value2 AND value3
{% endhighlight %}
      </td>
      <td>
        <p>By default (or with the ASYMMETRIC keyword), returns TRUE if <i>value1</i> is less than <i>value2</i> or greater than <i>value3</i>.
          With the SYMMETRIC keyword, returns TRUE if <i>value1</i> is not inclusively between <i>value2</i> and <i>value3</i>. 
          When either <i>value2</i> or <i>value3</i> is NULL, returns TRUE or UNKNOWN.</p>
          <p>E.g., <code>12 NOT BETWEEN 15 AND 12</code> returns TRUE;
          <code>12 NOT BETWEEN SYMMETRIC 15 AND 12</code> returns FALSE;
          <code>12 NOT BETWEEN NULL AND 15</code> returns UNKNOWN;
          <code>12 NOT BETWEEN 15 AND NULL</code> returns TRUE;
          <code>12 NOT BETWEEN SYMMETRIC 12 AND NULL</code> returns UNKNOWN.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
string1 LIKE string2 [ ESCAPE char ]
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if <i>string1</i> matches pattern <i>string2</i>; returns UNKNOWN if <i>string1</i> or <i>string2</i> is NULL. An escape character can be defined if necessary.</p>
        <p><b>Note:</b> The escape character has not been supported yet.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
string1 NOT LIKE string2 [ ESCAPE char ]
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if <i>string1</i> does not match pattern <i>string2</i>; returns UNKNOWN if <i>string1</i> or <i>string2</i> is NULL. An escape character can be defined if necessary.</p>
        <p><b>Note:</b> The escape character has not been supported yet.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
string1 SIMILAR TO string2 [ ESCAPE char ]
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if <i>string1</i> matches SQL regular expression <i>string2</i>; returns UNKNOWN if <i>string1</i> or <i>string2</i> is NULL. An escape character can be defined if necessary.</p>
        <p><b>Note:</b> The escape character has not been supported yet.</p>
      </td>
    </tr>


    <tr>
      <td>
        {% highlight text %}
string1 NOT SIMILAR TO string2 [ ESCAPE char ]
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if <i>string1</i> does not match SQL regular expression <i>string2</i>; returns UNKNOWN if <i>string1</i> or <i>string2</i> is NULL. An escape character can be defined if necessary.</p>
        <p><b>Note:</b> The escape character has not been supported yet.</p>
      </td>
    </tr>


    <tr>
      <td>
        {% highlight text %}
value1 IN (value2 [, value3]* )
{% endhighlight %}
      </td>
      <td>
        <p> Returns TRUE if <i>value1</i> exists in the given list <i>(value2, value3, ...)</i>. 
        When <i>(value2, value3, ...)</i>. contains NULL, returns TRUE if the element can be found and UNKNOWN otherwise. Always returns UNKNOWN if <i>value1</i> is NULL.</p>
        <p>E.g., <code>4 IN (1, 2, 3)</code> returns FALSE;
        <code>1 IN (1, 2, NULL)</code> returns TRUE;
        <code>4 IN (1, 2, NULL)</code> returns UNKNOWN.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
value1 NOT IN (value2 [, value3]* )
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if <i>value1</i> does not exist in the given list <i>(value2, value3, ...)</i>.
        When <i>(value2, value3, ...)</i>. contains NULL, returns FALSE if <i>value1</i> can be found and UNKNOWN otherwise. Always returns UNKNOWN if <i>value1</i> is NULL.</p>
        <p>E.g., <code>4 NOT IN (1, 2, 3)</code> returns TRUE;
        <code>1 NOT IN (1, 2, NULL)</code> returns FALSE;
        <code>4 NOT IN (1, 2, NULL)</code> returns UNKNOWN.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
EXISTS (sub-query)
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if <i>sub-query</i> returns at least one row. Only supported if the operation can be rewritten in a join and group operation.</p>
        <p><b>Note:</b> For streaming queries the operation is rewritten in a join and group operation. The required state to compute the query result might grow infinitely depending on the number of distinct input rows. Please provide a query configuration with valid retention interval to prevent excessive state size. See <a href="/dev/table/streaming/query_configuration.html">Query Configuration</a> for details.</p>
      </td>
    </tr>

    <tr>
      <td>
{% highlight text %}
value IN (sub-query)
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if <i>value</i> is equal to a row returned by sub-query.</p>
        <p><b>Note:</b> For streaming queries the operation is rewritten in a join and group operation. The required state to compute the query result might grow infinitely depending on the number of distinct input rows. Please provide a query configuration with valid retention interval to prevent excessive state size. See <a href="/dev/table/streaming/query_configuration.html">Query Configuration</a> for details.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
value NOT IN (sub-query)
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if <i>value</i> is not equal to every row returned by <i>sub-query</i>.</p>
        <p><b>Note:</b> For streaming queries the operation is rewritten in a join and group operation. The required state to compute the query result might grow infinitely depending on the number of distinct input rows. Please provide a query configuration with valid retention interval to prevent excessive state size. See <a href="/dev/table/streaming/query_configuration.html">Query Configuration</a> for details.</p>
      </td>
    </tr>
    </tbody>
</table>
</div>

<div data-lang="Java/Python" markdown="1">
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
        {% highlight java %}
ANY1 === ANY2
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if <i>ANY1</i> is equal to <i>ANY2</i>; returns UNKNOWN if <i>ANY1</i> or <i>ANY2</i> is NULL.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
ANY1 !== ANY2
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if <i>ANY1</i> is not equal to <i>ANY2</i>; returns UNKNOWN if <i>ANY1</i> or <i>ANY2</i> is NULL. </p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
ANY1 > ANY2
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if <i>ANY1</i> is greater than <i>ANY2</i>; returns UNKNOWN if <i>ANY1</i> or <i>ANY2</i> is NULL.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
ANY1 >= ANY2
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if <i>ANY1</i> is greater than or equal to <i>ANY2</i>; returns UNKNOWN if <i>ANY1</i> or <i>ANY2</i> is NULL.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
ANY1 < ANY2
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if <i>ANY1</i> is less than <i>ANY2</i>; returns UNKNOWN if <i>ANY1</i> or <i>ANY2</i> is NULL.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
ANY1 <= ANY2
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if <i>ANY1</i> is less than or equal to <i>ANY2</i>; returns UNKNOWN if <i>ANY1</i> or <i>ANY2</i> is NULL.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
ANY.isNull
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if <i>ANY</i> is NULL.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
ANY.isNotNull
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if <i>ANY</i> is not NULL.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
STRING1.like(STRING2)
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if <i>STRING1</i> matches pattern <i>STRING2</i>; returns UNKNOWN if <i>STRING1</i> or <i>STRING2</i> is NULL.</p>
        <p>E.g., <code>"JoKn".like("Jo_n%")</code> returns TRUE.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
STRING1.similar(STRING2)
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if <i>STRING1</i> matches SQL regular expression <i>STRING2</i>; returns UNKNOWN if <i>STRING1</i> or <i>STRING2</i> is NULL.</p>
        <p>E.g., <code>"A".similar("A+")</code> returns TRUE.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
ANY1.in(ANY2, ANY3, ...)
{% endhighlight %}
      </td>
      <td>
        <p> Returns TRUE if <i>ANY1</i> exists in a given list <i>(ANY2, ANY3, ...)</i>. 
        When <i>(ANY2, ANY3, ...)</i>. contains NULL, returns TRUE if the element can be found and UNKNOWN otherwise. Always returns UNKNOWN if <i>ANY1</i> is NULL.</p>
        <p>E.g., <code>4.in(1, 2, 3)</code> returns FALSE.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
ANY.in(TABLE)
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if <i>ANY</i> is equal to a row returned by sub-query <i>TABLE</i>.</p>
        <p><b>Note:</b> For streaming queries the operation is rewritten in a join and group operation. The required state to compute the query result might grow infinitely depending on the number of distinct input rows. Please provide a query configuration with valid retention interval to prevent excessive state size. See <a href="/dev/table/streaming/query_configuration.html">Query Configuration</a> for details.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
ANY1.between(ANY2, ANY3)
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if <i>ANY1</i> is greater than or equal to <i>ANY2</i> and less than or equal to <i>ANY3</i>.
          When either <i>ANY2</i> or <i>ANY3</i> is NULL, returns FALSE or UNKNOWN.</p>
          <p>E.g., <code>12.between(15, 12)</code> returns FALSE;
          <code>12.between(10, Null(INT))</code> returns UNKNOWN;
          <code>12.between(Null(INT), 10)</code> returns FALSE.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
ANY1.notBetween(ANY2, ANY3)
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if <i>ANY1</i> is less than <i>ANY2</i> or greater than <i>ANY3</i>.
          When either <i>ANY2</i> or <i>ANY3</i> is NULL, returns TRUE or UNKNOWN.</p>
          <p>E.g., <code>12.notBetween(15, 12)</code> returns TRUE;
          <code>12.notBetween(Null(INT), 15)</code> returns UNKNOWN;
          <code>12.notBetween(15, Null(INT))</code> returns TRUE.</p>
      </td>
    </tr>
  </tbody>
</table>
</div>

<div data-lang="scala" markdown="1">
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
        {% highlight scala %}
ANY1 === ANY2
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if <i>ANY1</i> is equal to <i>ANY2</i>; returns UNKNOWN if <i>ANY1</i> or <i>ANY2</i> is NULL.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
ANY1 !== ANY2
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if <i>ANY1</i> is not equal to <i>ANY2</i>; returns UNKNOWN if <i>ANY1</i> or <i>ANY2</i> is NULL. </p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
ANY1 > ANY2
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if <i>ANY1</i> is greater than <i>ANY2</i>; returns UNKNOWN if <i>ANY1</i> or <i>ANY2</i> is NULL.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
ANY1 >= ANY2
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if <i>ANY1</i> is greater than or equal to <i>ANY2</i>; returns UNKNOWN if <i>ANY1</i> or <i>ANY2</i> is NULL.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
ANY1 < ANY2
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if <i>ANY1</i> is less than <i>ANY2</i>; returns UNKNOWN if <i>ANY1</i> or <i>ANY2</i> is NULL.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
ANY1 <= ANY2
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if <i>ANY1</i> is less than or equal to <i>ANY2</i>; returns UNKNOWN if <i>ANY1</i> or <i>ANY2</i> is NULL.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
ANY.isNull
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if <i>ANY</i> is NULL.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
ANY.isNotNull
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if <i>ANY</i> is not NULL.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
STRING1.like(STRING2)
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if <i>STRING1</i> matches pattern <i>STRING2</i>; returns UNKNOWN if <i>STRING1</i> or <i>STRING2</i> is NULL.</p>
        <p>E.g., <code>"JoKn".like("Jo_n%")</code> returns TRUE.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
STRING1.similar(STRING2)
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if <i>STRING1</i> matches SQL regular expression <i>STRING2</i>; returns UNKNOWN if <i>STRING1</i> or <i>STRING2</i> is NULL.</p>
        <p>E.g., <code>"A".similar("A+")</code> returns TRUE.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
ANY1.in(ANY2, ANY3, ...)
{% endhighlight %}
      </td>
      <td>
        <p> Returns TRUE if <i>ANY1</i> exists in a given list <i>(ANY2, ANY3, ...)</i>. 
        When <i>(ANY2, ANY3, ...)</i>. contains NULL, returns TRUE if the element can be found and UNKNOWN otherwise. Always returns UNKNOWN if <i>ANY1</i> is NULL.</p>
        <p>E.g., <code>4.in(1, 2, 3)</code> returns FALSE.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
ANY.in(TABLE)
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if <i>ANY</i> is equal to a row returned by sub-query <i>TABLE</i>.</p>
        <p><b>Note:</b> For streaming queries the operation is rewritten in a join and group operation. The required state to compute the query result might grow infinitely depending on the number of distinct input rows. Please provide a query configuration with valid retention interval to prevent excessive state size. See <a href="/dev/table/streaming/query_configuration.html">Query Configuration</a> for details.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
ANY1.between(ANY2, ANY3)
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if <i>ANY1</i> is greater than or equal to <i>ANY2</i> and less than or equal to <i>ANY3</i>.
          When either <i>ANY2</i> or <i>ANY3</i> is NULL, returns FALSE or UNKNOWN.</p>
          <p>E.g., <code>12.between(15, 12)</code> returns FALSE;
          <code>12.between(10, Null(Types.INT))</code> returns UNKNOWN;
          <code>12.between(Null(Types.INT), 10)</code> returns FALSE.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
ANY1.notBetween(ANY2, ANY3)
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if <i>ANY1</i> is less than <i>ANY2</i> or greater than <i>ANY3</i>.
          When either <i>ANY2</i> or <i>ANY3</i> is NULL, returns TRUE or UNKNOWN.</p>
          <p>E.g., <code>12.notBetween(15, 12)</code> returns TRUE;
          <code>12.notBetween(Null(Types.INT), 15)</code> returns UNKNOWN;
          <code>12.notBetween(15, Null(Types.INT))</code> returns TRUE.</p>
      </td>
    </tr>
  </tbody>
</table>
</div>
</div>

{% top %}

### Logical Functions

<div class="codetabs" markdown="1">
<div data-lang="SQL" markdown="1">
<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Logical functions</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>

  <tbody>
    <tr>
      <td>
        {% highlight text %}
boolean1 OR boolean2
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if <i>boolean1</i> is TRUE or <i>boolean2</i> is TRUE. Supports three-valued logic.</p>
        <p>E.g., <code>TRUE OR UNKNOWN</code> returns TRUE.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
boolean1 AND boolean2
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if <i>boolean1</i> and <i>boolean2</i> are both TRUE. Supports three-valued logic.</p>
        <p>E.g., <code>TRUE AND UNKNOWN</code> returns UNKNOWN.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
NOT boolean
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if <i>boolean</i> is FALSE; returns FALSE if <i>boolean</i> is TRUE; returns UNKNOWN if <i>boolean</i> is UNKNOWN.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
boolean IS FALSE
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if <i>boolean</i> is FALSE; returns FALSE if <i>boolean</i> is TRUE or UNKNOWN.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
boolean IS NOT FALSE
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if <i>boolean</i> is TRUE or UNKNOWN; returns FALSE if <i>boolean</i> is FALSE.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
boolean IS TRUE
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if <i>boolean</i> is TRUE; returns FALSE if <i>boolean</i> is FALSE or UNKNOWN.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
boolean IS NOT TRUE
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if <i>boolean</i> is FALSE or UNKNOWN; returns FALSE if <i>boolean</i> is TRUE.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
boolean IS UNKNOWN
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if <i>boolean</i> is UNKNOWN; returns FALSE if <i>boolean</i> is TRUE or FALSE.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
boolean IS NOT UNKNOWN
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if <i>boolean</i> is TRUE or FALSE; returns FALSE if <i>boolean</i> is UNKNOWN.</p>
      </td>
    </tr>
  </tbody>
</table>
</div>

<div data-lang="Java/Python" markdown="1">
<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Logical functions</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>

  <tbody>
    <tr>
      <td>
        {% highlight java %}
BOOLEAN1 || BOOLEAN2
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if <i>BOOLEAN1</i> is TRUE or <i>BOOLEAN2</i> is TRUE. Supports three-valued logic.</p>
        <p>E.g., <code>true || Null(BOOLEAN)</code> returns TRUE.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
BOOLEAN1 && BOOLEAN2
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if <i>BOOLEAN1</i> and <i>BOOLEAN2</i> are both TRUE. Supports three-valued logic.</p>
        <p>E.g., <code>true && Null(BOOLEAN)</code> returns UNKNOWN.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
!BOOLEAN
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if <i>BOOLEAN</i> is FALSE; returns FALSE if <i>BOOLEAN</i> is TRUE; returns UNKNOWN if <i>BOOLEAN</i> is UNKNOWN.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
BOOLEAN.isTrue
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if <i>BOOLEAN</i> is TRUE; returns FALSE if <i>BOOLEAN</i> is FALSE or UNKNOWN.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
BOOLEAN.isFalse
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if <i>BOOLEAN</i> is FALSE; returns FALSE if <i>BOOLEAN</i> is TRUE or UNKNOWN.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
BOOLEAN.isNotTrue
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if <i>BOOLEAN</i> is FALSE or UNKNOWN; returns FALSE if <i>BOOLEAN</i> is TRUE.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
BOOLEAN.isNotFalse
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if <i>BOOLEAN</i> is TRUE or UNKNOWN; returns FALSE if <i>BOOLEAN</i> is FALSE.</p>
      </td>
    </tr>
  </tbody>
</table>
</div>

<div data-lang="scala" markdown="1">
<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Logical functions</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>

  <tbody>
    <tr>
      <td>
        {% highlight scala %}
BOOLEAN1 || BOOLEAN2
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if <i>BOOLEAN1</i> is TRUE or <i>BOOLEAN2</i> is TRUE. Supports three-valued logic.</p>
        <p>E.g., <code>true || Null(Types.BOOLEAN)</code> returns TRUE.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
BOOLEAN1 && BOOLEAN2
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if <i>BOOLEAN1</i> and <i>BOOLEAN2</i> are both TRUE. Supports three-valued logic.</p>
        <p>E.g., <code>true && Null(Types.BOOLEAN)</code> returns UNKNOWN.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
!BOOLEAN
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if <i>BOOLEAN</i> is FALSE; returns FALSE if <i>BOOLEAN</i> is TRUE; returns UNKNOWN if <i>BOOLEAN</i> is UNKNOWN.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
BOOLEAN.isTrue
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if <i>BOOLEAN</i> is TRUE; returns FALSE if <i>BOOLEAN</i> is FALSE or UNKNOWN.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
BOOLEAN.isFalse
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if <i>BOOLEAN</i> is FALSE; returns FALSE if <i>BOOLEAN</i> is TRUE or UNKNOWN.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
BOOLEAN.isNotTrue
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if <i>BOOLEAN</i> is FALSE or UNKNOWN; returns FALSE if <i>BOOLEAN</i> is TRUE.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
BOOLEAN.isNotFalse
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if <i>BOOLEAN</i> is TRUE or UNKNOWN; returns FALSE if <i>BOOLEAN</i> is FALSE.</p>
      </td>
    </tr>
  </tbody>
</table>
</div>
</div>

{% top %}

### Arithmetic Functions

<div class="codetabs" markdown="1">
<div data-lang="SQL" markdown="1">
<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Arithmetic functions</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>

  <tbody>
    <tr>
      <td>
        {% highlight text %}
+ numeric
{% endhighlight %}
      </td>
      <td>
        <p>Returns <i>numeric</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
- numeric
{% endhighlight %}
      </td>
      <td>
        <p>Returns negative <i>numeric</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
numeric1 + numeric2
{% endhighlight %}
      </td>
      <td>
        <p>Returns <i>numeric1</i> plus <i>numeric2</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
numeric1 - numeric2
{% endhighlight %}
      </td>
      <td>
        <p>Returns <i>numeric1</i> minus <i>numeric2</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
numeric1 * numeric2
{% endhighlight %}
      </td>
      <td>
        <p>Returns <i>numeric1</i> multiplied by <i>numeric2</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
numeric1 / numeric2
{% endhighlight %}
      </td>
      <td>
        <p>Returns <i>numeric1</i> divided by <i>numeric2</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
POWER(numeric1, numeric2)
{% endhighlight %}
      </td>
      <td>
        <p>Returns <i>numeric1</i> raised to the power of <i>numeric2</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
ABS(numeric)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the absolute value of <i>numeric</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
MOD(numeric1, numeric2)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the remainder (modulus) of <i>numeric1</i> divided by <i>numeric2</i>. The result is negative only if <i>numeric1</i> is negative.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
SQRT(numeric)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the square root of <i>numeric</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
LN(numeric)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the natural logarithm (base e) of <i>numeric</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
LOG10(numeric)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the base 10 logarithm of <i>numeric</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
LOG2(numeric)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the base 2 logarithm of <i>numeric</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
       {% highlight text %}
LOG(numeric2)
LOG(numeric1, numeric2)
{% endhighlight %}
      </td>
      <td>
        <p>When called with one argument, returns the natural logarithm of <i>numeric2</i>. When called with two arguments, this function returns the logarithm of <i>numeric2</i> to the base <i>numeric1</i>.</p> 
        <p><b>Note:</b> Currently, <i>numeric2</i> must be greater than 0 and <i>numeric1</i> must be greater than 1.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
EXP(numeric)
{% endhighlight %}
      </td>
      <td>
        <p>Returns e raised to the power of <i>numeric</i>.</p>
      </td>
    </tr>   

    <tr>
      <td>
        {% highlight text %}
CEIL(numeric)
CEILING(numeric)
{% endhighlight %}
      </td>
      <td>
        <p>Rounds <i>numeric</i> up, and returns the smallest number that is greater than or equal to <i>numeric</i>.</p>
      </td>
    </tr>  

    <tr>
      <td>
        {% highlight text %}
FLOOR(numeric)
{% endhighlight %}
      </td>
      <td>
        <p>Rounds <i>numeric</i> down, and returns the largest number that is less than or equal to <i>numeric</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
SIN(numeric)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the sine of <i>numeric</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
SINH(numeric)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the hyperbolic sine of <i>numeric</i>.</p> 
        <p>The return type is <i>DOUBLE</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
COS(numeric)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the cosine of <i>numeric</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
TAN(numeric)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the tangent of <i>numeric</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
TANH(numeric)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the hyperbolic tangent of <i>numeric</i>.</p> 
        <p>The return type is <i>DOUBLE</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
COT(numeric)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the cotangent of a <i>numeric</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
ASIN(numeric)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the arc sine of <i>numeric</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
ACOS(numeric)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the arc cosine of <i>numeric</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
ATAN(numeric)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the arc tangent of <i>numeric</i>.</p>
      </td>
    </tr>
    
    <tr>
      <td>
        {% highlight text %}
ATAN2(numeric1, numeric2)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the arc tangent of a coordinate <i>(numeric1, numeric2)</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
COSH(numeric)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the hyperbolic cosine of <i>NUMERIC</i>.</p> 
        <p>Return value type is <i>DOUBLE</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
DEGREES(numeric)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the degree representation of a radian <i>numeric</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
RADIANS(numeric)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the radian representation of a degree <i>numeric</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
SIGN(numeric)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the signum of <i>numeric</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
ROUND(numeric, integer)
{% endhighlight %}
      </td>
      <td>
        <p>Returns a number rounded to <i>integer</i> decimal places for <i>numeric</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
PI
{% endhighlight %}
      </td>
      <td>
        <p>Returns a value that is closer than any other values to pi.</p>
      </td>
    </tr>
    <tr>
      <td>
        {% highlight text %}
E()
{% endhighlight %}
      </td>
      <td>
        <p>Returns a value that is closer than any other values to e.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
RAND()
{% endhighlight %}
      </td>
      <td>
        <p>Returns a pseudorandom double value between 0.0 (inclusive) and 1.0 (exclusive).</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
RAND(integer)
{% endhighlight %}
      </td>
      <td>
        <p>Returns a pseudorandom double value between 0.0 (inclusive) and 1.0 (exclusive) with an initial seed <i>integer</i>. Two RAND functions will return identical sequences of numbers if they have the same initial seed.</p>
      </td>
    </tr>

    <tr>
     <td>
       {% highlight text %}
RAND_INTEGER(integer)
{% endhighlight %}
     </td>
    <td>
      <p>Returns a pseudorandom integer value between 0 (inclusive) and <i>integer</i> (exclusive).</p>
    </td>
   </tr>

    <tr>
     <td>
       {% highlight text %}
RAND_INTEGER(integer1, integer2)
{% endhighlight %}
     </td>
    <td>
      <p>Returns a pseudorandom integer value between 0 (inclusive) and the specified value (exclusive) with an initial seed. Two RAND_INTEGER functions will return identical sequences of numbers if they have the same initial seed and bound.</p>
    </td>
   </tr>

    <tr>
     <td>
       {% highlight text %}
UUID()
{% endhighlight %}
     </td>
    <td>
      <p>Returns an UUID (Universally Unique Identifier) string (e.g., "3d3c68f7-f608-473f-b60c-b0c44ad4cc4e") according to RFC 4122 type 4 (pseudo randomly generated) UUID. The UUID is generated using a cryptographically strong pseudo random number generator.</p>
    </td>
   </tr>
    
    <tr>
      <td>
        {% highlight text %}
BIN(integer)
{% endhighlight %}
      </td>
      <td>
        <p>Returns a string representation of <i>integer</i> in binary format. Returns NULL if <i>integer</i> is NULL.</p>
        <p>E.g. <code>BIN(4)</code> returns '100' and <code>BIN(12)</code> returns '1100'.</p>
      </td>
    </tr>

    <tr>
      <td>
{% highlight text %}
HEX(numeric)
HEX(string)
      {% endhighlight %}
      </td>
      <td>
        <p>Returns a string representation of an integer <i>numeric</i> value or a <i>string</i> in hex format. Returns NULL if the argument is NULL.</p>
        <p>E.g. a numeric 20 leads to "14", a numeric 100 leads to "64", a string "hello,world" leads to "68656C6C6F2C776F726C64".</p>
      </td>
    </tr>
        
    <tr>
      <td>
        {% highlight text %}
TRUNCATE(numeric1, integer2)
{% endhighlight %}
      </td>
      <td>
        <p>Returns a <i>numeric</i> of truncated to <i>integer2</i> decimal places. Returns NULL if <i>numeric1</i> or <i>integer2</i> is NULL.If <i>integer2</i> is 0,the result has no decimal point or fractional part.<i>integer2</i> can be negative to cause <i>integer2</i> digits left of the decimal point of the value to become zero.This function can also pass in only one <i>numeric1</i> parameter and not set <i>Integer2</i> to use.If <i>Integer2</i> is not set, the function truncates as if <i>Integer2</i> were 0.</p>
        <p>E.g. <code>truncate(42.345, 2)</code> to 42.34. and <code>truncate(42.345)</code> to 42.0.</p>
      </td>
    </tr>
    
    <tr>
      <td>
        {% highlight text %}
PI()
{% endhighlight %}
      </td>
      <td>
      <p>Returns the value of π (pi).</p>
      <p>Only supported in blink planner.</p>
      </td>
    </tr> 
        
  </tbody>
</table>
</div>

<div data-lang="Java/Python" markdown="1">
<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Arithmetic functions</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>

  <tbody>
   <tr>
      <td>
        {% highlight java %}
+ NUMERIC
{% endhighlight %}
      </td>
      <td>
        <p>Returns <i>NUMERIC</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
- NUMERIC
{% endhighlight %}
      </td>
      <td>
        <p>Returns negative <i>NUMERIC</i>.</p>
      </td>
    </tr>
    
    <tr>
      <td>
        {% highlight java %}
NUMERIC1 + NUMERIC2
{% endhighlight %}
      </td>
      <td>
        <p>Returns <i>NUMERIC1</i> plus <i>NUMERIC2</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
NUMERIC1 - NUMERIC2
{% endhighlight %}
      </td>
      <td>
        <p>Returns <i>NUMERIC1</i> minus <i>NUMERIC2</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
NUMERIC1 * NUMERIC2
{% endhighlight %}
      </td>
      <td>
        <p>Returns <i>NUMERIC1</i> multiplied by <i>NUMERIC2</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
NUMERIC1 / NUMERIC2
{% endhighlight %}
      </td>
      <td>
        <p>Returns <i>NUMERIC1</i> divided by <i>NUMERIC2</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
NUMERIC1.power(NUMERIC2)
{% endhighlight %}
      </td>
      <td>
        <p>Returns <i>NUMERIC1</i> raised to the power of <i>NUMERIC2</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
NUMERIC.abs()
{% endhighlight %}
      </td>
      <td>
        <p>Returns the absolute value of <i>NUMERIC</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
NUMERIC1 % NUMERIC2
{% endhighlight %}
      </td>
      <td>
        <p>Returns the remainder (modulus) of <i>NUMERIC1</i> divided by <i>NUMERIC2</i>. The result is negative only if <i>numeric1</i> is negative.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
NUMERIC.sqrt()
{% endhighlight %}
      </td>
      <td>
        <p>Returns the square root of <i>NUMERIC</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
NUMERIC.ln()
{% endhighlight %}
      </td>
      <td>
        <p>Returns the natural logarithm (base e) of <i>NUMERIC</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
NUMERIC.log10()
{% endhighlight %}
      </td>
      <td>
        <p>Returns the base 10 logarithm of <i>NUMERIC</i>.</p>
      </td>
    </tr>
    
    <tr>
      <td>
        {% highlight java %}
NUMERIC.log2()
{% endhighlight %}
      </td>
      <td>
        <p>Returns the base 2 logarithm of <i>NUMERIC</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
NUMERIC1.log()
NUMERIC1.log(NUMERIC2)
{% endhighlight %}
      </td>
      <td>
        <p>When called without argument, returns the natural logarithm of <i>NUMERIC1</i>. When called with an argument, returns the logarithm of <i>NUMERIC1</i> to the base <i>NUMERIC2</i>.</p> 
        <p><b>Note:</b> Currently, <i>NUMERIC1</i> must be greater than 0 and <i>NUMERIC2</i> must be greater than 1.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
NUMERIC.exp()
{% endhighlight %}
      </td>
      <td>
        <p>Returns e raised to the power of <i>NUMERIC</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
NUMERIC.ceil()
{% endhighlight %}
      </td>
      <td>
        <p>Rounds <i>NUMERIC</i> up, and returns the smallest number that is greater than or equal to <i>NUMERIC</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
NUMERIC.floor()
{% endhighlight %}
      </td>
      <td>
        <p>Rounds <i>NUMERIC</i> down, and returns the largest number that is less than or equal to <i>NUMERIC</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
NUMERIC.sin()
{% endhighlight %}
      </td>
      <td>
        <p>Returns the sine of <i>NUMERIC</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
NUMERIC.sinh()
{% endhighlight %}
      </td>
      <td>
        <p>Returns the hyperbolic sine of <i>NUMERIC</i>.</p> 
        <p>The return type is <i>DOUBLE</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
NUMERIC.cos()
{% endhighlight %}
      </td>
      <td>
        <p>Returns the cosine of <i>NUMERIC</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
NUMERIC.tan()
{% endhighlight %}
      </td>
      <td>
        <p>Returns the tangent of <i>NUMERIC</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
NUMERIC.tanh()
{% endhighlight %}
      </td>
      <td>
        <p>Returns the hyperbolic tangent of <i>NUMERIC</i>.</p> 
        <p>The return type is <i>DOUBLE</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
NUMERIC.cot()
{% endhighlight %}
      </td>
      <td>
        <p>Returns the cotangent of a <i>NUMERIC</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
NUMERIC.asin()
{% endhighlight %}
      </td>
      <td>
        <p>Returns the arc sine of <i>NUMERIC</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
NUMERIC.acos()
{% endhighlight %}
      </td>
      <td>
        <p>Returns the arc cosine of <i>NUMERIC</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
NUMERIC.atan()
{% endhighlight %}
      </td>
      <td>
        <p>Returns the arc tangent of <i>NUMERIC</i>.</p>
      </td>
    </tr>
    
    <tr>
      <td>
        {% highlight java %}
atan2(NUMERIC1, NUMERIC2)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the arc tangent of a coordinate <i>(NUMERIC1, NUMERIC2)</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
NUMERIC.cosh()
{% endhighlight %}
      </td>
      <td>
        <p>Returns the hyperbolic cosine of <i>NUMERIC</i>.</p> 
        <p>Return value type is <i>DOUBLE</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
NUMERIC.degrees()
{% endhighlight %}
      </td>
      <td>
        <p>Returns the degree representation of a radian <i>NUMERIC</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
NUMERIC.radians()
{% endhighlight %}
      </td>
      <td>
        <p>Returns the radian representation of a degree <i>NUMERIC</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
NUMERIC.sign()
{% endhighlight %}
      </td>
      <td>
        <p>Returns the signum of <i>NUMERIC</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
NUMERIC.round(INT)
{% endhighlight %}
      </td>
      <td>
        <p>Returns a number rounded to <i>INT</i> decimal places for <i>NUMERIC</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
pi()
{% endhighlight %}
      </td>
      <td>
        <p>Returns a value that is closer than any other values to pi.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
e()
{% endhighlight %}
      </td>
      <td>
        <p>Returns a value that is closer than any other values to e.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
rand()
{% endhighlight %}
      </td>
      <td>
        <p>Returns a pseudorandom double value between 0.0 (inclusive) and 1.0 (exclusive).</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
rand(INTEGER)
{% endhighlight %}
      </td>
      <td>
        <p>Returns a pseudorandom double value between 0.0 (inclusive) and 1.0 (exclusive) with an initial seed <i>INTEGER</i>. Two RAND functions will return identical sequences of numbers if they have the same initial seed.</p>
      </td>
    </tr>

    <tr>
     <td>
       {% highlight java %}
randInteger(INTEGER)
{% endhighlight %}
     </td>
    <td>
      <p>Returns a pseudorandom integer value between 0 (inclusive) and <i>INTEGER</i> (exclusive).</p>
    </td>
   </tr>

    <tr>
     <td>
       {% highlight java %}
randInteger(INTEGER1, INTEGER2)
{% endhighlight %}
     </td>
    <td>
      <p>Returns a pseudorandom integer value between 0 (inclusive) and <i>INTEGER2</i> (exclusive) with an initial seed <i>INTEGER1</i>. Two randInteger functions will return identical sequences of numbers if they have same initial seed and bound.</p>
    </td>
   </tr>

    <tr>
     <td>
       {% highlight java %}
uuid()
{% endhighlight %}
     </td>
    <td>
      <p>Returns an UUID (Universally Unique Identifier) string (e.g., "3d3c68f7-f608-473f-b60c-b0c44ad4cc4e") according to RFC 4122 type 4 (pseudo randomly generated) UUID. The UUID is generated using a cryptographically strong pseudo random number generator.</p>
    </td>
   </tr>

    <tr>
     <td>
       {% highlight java %}
INTEGER.bin()
{% endhighlight %}
     </td>
    <td>
      <p>Returns a string representation of <i>INTEGER</i> in binary format. Returns NULL if <i>INTEGER</i> is NULL.</p>
      <p>E.g., <code>4.bin()</code> returns "100" and <code>12.bin()</code> returns "1100".</p>
    </td>
   </tr>

    <tr>
      <td>
       {% highlight java %}
NUMERIC.hex()
STRING.hex()
{% endhighlight %}
     </td>
    <td>
      <p>Returns a string representation of an integer <i>NUMERIC</i> value or a <i>STRING</i> in hex format. Returns NULL if the argument is NULL.</p>
      <p>E.g. a numeric 20 leads to "14", a numeric 100 leads to "64", a string "hello,world" leads to "68656C6C6F2C776F726C64".</p>
    </td>
   </tr>
 
       <tr>
         <td>
           {% highlight text %}
numeric1.truncate(INTEGER2)
   {% endhighlight %}
         </td>
         <td>
           <p>Returns a <i>numeric</i> of truncated to <i>integer2</i> decimal places. Returns NULL if <i>numeric1</i> or <i>integer2</i> is NULL.If <i>integer2</i> is 0,the result has no decimal point or fractional part.<i>integer2</i> can be negative to cause <i>integer2</i> digits left of the decimal point of the value to become zero.This function can also pass in only one <i>numeric1</i> parameter and not set <i>Integer2</i> to use.If <i>Integer2</i> is not set, the function truncates as if <i>Integer2</i> were 0.</p>
           <p>E.g. <code>42.324.truncate(2)</code> to 42.34. and <code>42.324.truncate()</code> to 42.0.</p>
         </td>
       </tr>
   
  </tbody>
</table>
</div>

<div data-lang="scala" markdown="1">
<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Arithmetic functions</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>

  <tbody>
   <tr>
      <td>
        {% highlight scala %}
+ NUMERIC
{% endhighlight %}
      </td>
      <td>
        <p>Returns <i>NUMERIC</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
- NUMERIC
{% endhighlight %}
      </td>
      <td>
        <p>Returns negative <i>NUMERIC</i>.</p>
      </td>
    </tr>
    
    <tr>
      <td>
        {% highlight scala %}
NUMERIC1 + NUMERIC2
{% endhighlight %}
      </td>
      <td>
        <p>Returns <i>NUMERIC1</i> plus <i>NUMERIC2</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
NUMERIC1 - NUMERIC2
{% endhighlight %}
      </td>
      <td>
        <p>Returns <i>NUMERIC1</i> minus <i>NUMERIC2</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
NUMERIC1 * NUMERIC2
{% endhighlight %}
      </td>
      <td>
        <p>Returns <i>NUMERIC1</i> multiplied by <i>NUMERIC2</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
NUMERIC1 / NUMERIC2
{% endhighlight %}
      </td>
      <td>
        <p>Returns <i>NUMERIC1</i> divided by <i>NUMERIC2</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
NUMERIC1.power(NUMERIC2)
{% endhighlight %}
      </td>
      <td>
        <p>Returns <i>NUMERIC1</i> raised to the power of <i>NUMERIC2</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
NUMERIC.abs()
{% endhighlight %}
      </td>
      <td>
        <p>Returns the absolute value of <i>NUMERIC</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
NUMERIC1 % NUMERIC2
{% endhighlight %}
      </td>
      <td>
        <p>Returns the remainder (modulus) of <i>NUMERIC1</i> divided by <i>NUMERIC2</i>. The result is negative only if <i>numeric1</i> is negative.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
NUMERIC.sqrt()
{% endhighlight %}
      </td>
      <td>
        <p>Returns the square root of <i>NUMERIC</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
NUMERIC.ln()
{% endhighlight %}
      </td>
      <td>
        <p>Returns the natural logarithm (base e) of <i>NUMERIC</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
NUMERIC.log10()
{% endhighlight %}
      </td>
      <td>
        <p>Returns the base 10 logarithm of <i>NUMERIC</i>.</p>
      </td>
    </tr>
    
    <tr>
      <td>
        {% highlight scala %}
NUMERIC.log2()
{% endhighlight %}
      </td>
      <td>
        <p>Returns the base 2 logarithm of <i>NUMERIC</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
NUMERIC1.log()
NUMERIC1.log(NUMERIC2)
{% endhighlight %}
      </td>
      <td>
        <p>When called without argument, returns the natural logarithm of <i>NUMERIC1</i>. When called with an argument, returns the logarithm of <i>NUMERIC1</i> to the base <i>NUMERIC2</i>.</p> 
        <p><b>Note:</b> Currently, <i>NUMERIC1</i> must be greater than 0 and <i>NUMERIC2</i> must be greater than 1.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
NUMERIC.exp()
{% endhighlight %}
      </td>
      <td>
        <p>Returns e raised to the power of <i>NUMERIC</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
NUMERIC.ceil()
{% endhighlight %}
      </td>
      <td>
        <p>Rounds <i>NUMERIC</i> up, and returns the smallest number that is greater than or equal to <i>NUMERIC</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
NUMERIC.floor()
{% endhighlight %}
      </td>
      <td>
        <p>Rounds <i>NUMERIC</i> down, and returns the largest number that is less than or equal to <i>NUMERIC</i>.</p>
      </td>
    </tr>
    
    <tr>
      <td>
        {% highlight scala %}
NUMERIC.sin()
{% endhighlight %}
      </td>
      <td>
        <p>Returns the sine of <i>NUMERIC</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
NUMERIC.sinh()
{% endhighlight %}
      </td>
      <td>
        <p>Returns the hyperbolic sine of <i>NUMERIC</i>.</p> 
        <p>The return type is <i>DOUBLE</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
NUMERIC.cos()
{% endhighlight %}
      </td>
      <td>
        <p>Returns the cosine of <i>NUMERIC</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
NUMERIC.tan()
{% endhighlight %}
      </td>
      <td>
        <p>Returns the tangent of <i>NUMERIC</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
NUMERIC.tanh()
{% endhighlight %}
      </td>
      <td>
        <p>Returns the hyperbolic tangent of <i>NUMERIC</i>.</p> 
        <p>The return type is <i>DOUBLE</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
NUMERIC.cot()
{% endhighlight %}
      </td>
      <td>
        <p>Returns the cotangent of a <i>NUMERIC</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
NUMERIC.asin()
{% endhighlight %}
      </td>
      <td>
        <p>Returns the arc sine of <i>NUMERIC</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
NUMERIC.acos()
{% endhighlight %}
      </td>
      <td>
        <p>Returns the arc cosine of <i>NUMERIC</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
NUMERIC.atan()
{% endhighlight %}
      </td>
      <td>
        <p>Returns the arc tangent of <i>NUMERIC</i>.</p>
      </td>
    </tr>
    
    <tr>
      <td>
        {% highlight scala %}
atan2(NUMERIC1, NUMERIC2)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the arc tangent of a coordinate <i>(NUMERIC1, NUMERIC2)</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
NUMERIC.cosh()
{% endhighlight %}
      </td>
      <td>
        <p>Returns the hyperbolic cosine of <i>NUMERIC</i>.</p> 
        <p>Return value type is <i>DOUBLE</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
NUMERIC.degrees()
{% endhighlight %}
      </td>
      <td>
        <p>Returns the degree representation of a radian <i>NUMERIC</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
NUMERIC.radians()
{% endhighlight %}
      </td>
      <td>
        <p>Returns the radian representation of a degree <i>NUMERIC</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
NUMERIC.sign()
{% endhighlight %}
      </td>
      <td>
        <p>Returns the signum of <i>NUMERIC</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
NUMERIC.round(INT)
{% endhighlight %}
      </td>
      <td>
        <p>Returns a number rounded to <i>INT</i> decimal places for <i>NUMERIC</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
pi()
{% endhighlight %}
      </td>
      <td>
        <p>Returns a value that is closer than any other values to pi.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
e()
{% endhighlight %}
      </td>
      <td>
        <p>Returns a value that is closer than any other values to e.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
rand()
{% endhighlight %}
      </td>
      <td>
        <p>Returns a pseudorandom double value between 0.0 (inclusive) and 1.0 (exclusive).</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
rand(INTEGER)
{% endhighlight %}
      </td>
      <td>
        <p>Returns a pseudorandom double value between 0.0 (inclusive) and 1.0 (exclusive) with an initial seed <i>INTEGER</i>. Two RAND functions will return identical sequences of numbers if they have the same initial seed.</p>
      </td>
    </tr>

    <tr>
     <td>
       {% highlight scala %}
randInteger(INTEGER)
{% endhighlight %}
     </td>
    <td>
      <p>Returns a pseudorandom integer value between 0 (inclusive) and <i>INTEGER</i> (exclusive).</p>
    </td>
   </tr>

    <tr>
     <td>
       {% highlight scala %}
randInteger(INTEGER1, INTEGER2)
{% endhighlight %}
     </td>
    <td>
      <p>Returns a pseudorandom integer value between 0 (inclusive) and <i>INTEGER2</i> (exclusive) with an initial seed <i>INTEGER1</i>. Two randInteger functions will return identical sequences of numbers if they have same initial seed and bound.</p>
    </td>
   </tr>

    <tr>
     <td>
       {% highlight scala %}
uuid()
{% endhighlight %}
     </td>
    <td>
      <p>Returns an UUID (Universally Unique Identifier) string (e.g., "3d3c68f7-f608-473f-b60c-b0c44ad4cc4e") according to RFC 4122 type 4 (pseudo randomly generated) UUID. The UUID is generated using a cryptographically strong pseudo random number generator.</p>
    </td>
   </tr>

    <tr>
     <td>
       {% highlight scala %}
INTEGER.bin()
{% endhighlight %}
     </td>
    <td>
      <p>Returns a string representation of <i>INTEGER</i> in binary format. Returns NULL if <i>INTEGER</i> is NULL.</p>
      <p>E.g., <code>4.bin()</code> returns "100" and <code>12.bin()</code> returns "1100".</p>
    </td>
   </tr>

    <tr>
      <td>
       {% highlight scala %}
NUMERIC.hex()
STRING.hex()
{% endhighlight %}
     </td>
    <td>
      <p>Returns a string representation of an integer <i>NUMERIC</i> value or a <i>STRING</i> in hex format. Returns NULL if the argument is NULL.</p>
      <p>E.g. a numeric 20 leads to "14", a numeric 100 leads to "64", a string "hello,world" leads to "68656C6C6F2C776F726C64".</p>
    </td>
   </tr>
  </tbody>
</table>
</div>
</div>

{% top %}

### String Functions

<div class="codetabs" markdown="1">
<div data-lang="SQL" markdown="1">
<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">String functions</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>

  <tbody>
    <tr>
      <td>
        {% highlight text %}
string1 || string2
{% endhighlight %}
      </td>
      <td>
        <p>Returns the concatenation of <i>string1</i> and <i>string2</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
CHAR_LENGTH(string)
CHARACTER_LENGTH(string)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the number of characters in <i>string</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
UPPER(string)
{% endhighlight %}
      </td>
      <td>
        <p>Returns <i>string</i> in uppercase.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
LOWER(string)
{% endhighlight %}
      </td>
      <td>
        <p>Returns <i>string</i> in lowercase.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
POSITION(string1 IN string2)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the position (start from 1) of the first occurrence of <i>string1</i> in <i>string2</i>; returns 0 if <i>string1</i> cannot be found in <i>string2</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
TRIM([ BOTH | LEADING | TRAILING ] string1 FROM string2)
{% endhighlight %}
      </td>
      <td>
        <p>Returns a string that removes leading and/or trailing characters <i>string1</i> from <i>string2</i>. By default, whitespaces at both sides are removed.</p>
      </td>
    </tr>
    
    <tr>
      <td>
        {% highlight text %}
LTRIM(string)
{% endhighlight %}
      </td>
      <td>
        <p>Returns a string that removes the left whitespaces from <i>string</i>.</p> 
        <p>E.g., <code>LTRIM(' This is a test String.')</code> returns "This is a test String.".</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
RTRIM(string)
{% endhighlight %}
      </td>
      <td>
        <p>Returns a string that removes the right whitespaces from <i>string</i>.</p> 
        <p>E.g., <code>RTRIM('This is a test String. ')</code> returns "This is a test String.".</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
REPEAT(string, integer)
{% endhighlight %}
      </td>
      <td>
        <p>Returns a string that repeats the base <i>string</i> <i>integer</i> times.</p> 
        <p>E.g., <code>REPEAT('This is a test String.', 2)</code> returns "This is a test String.This is a test String.".</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
REGEXP_REPLACE(string1, string2, string3)
{% endhighlight %}
      </td>
      <td>
        <p>Returns a string from <i>string1</i> with all the substrings that match a regular expression <i>string2</i> consecutively being replaced with <i>string3</i>.</p> 
        <p>E.g., <code>REGEXP_REPLACE('foobar', 'oo|ar', '')</code> returns "fb".</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
OVERLAY(string1 PLACING string2 FROM integer1 [ FOR integer2 ])
{% endhighlight %}
      </td>
      <td>
        <p>Returns a string that replaces <i>integer2</i> (<i>string2</i>'s length by default) characters of <i>string1</i> with <i>string2</i> from position <i>integer1</i>.</p>
        <p>E.g., <code>OVERLAY('This is an old string' PLACING ' new' FROM 10 FOR 5)</code> returns "This is a new string"</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
SUBSTRING(string FROM integer1 [ FOR integer2 ])
{% endhighlight %}
      </td>
      <td>
        <p>Returns a substring of <i>string</i> starting from position <i>integer1</i> with length <i>integer2</i> (to the end by default).</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
REPLACE(string1, string2, string3)
{% endhighlight %}
      </td>
      <td>
        <p>Returns a new string which replaces all the occurrences of <i>string2</i> with <i>string3</i> (non-overlapping) from <i>string1</i></p>
        <p>E.g., <code>REPLACE('hello world', 'world', 'flink')</code> returns "hello flink"; <code>REPLACE('ababab', 'abab', 'z')</code> returns "zab".</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
REGEXP_EXTRACT(string1, string2[, integer])
{% endhighlight %}
      </td>
      <td>
        <p>Returns a string from <i>string1</i> which extracted with a specified regular expression <i>string2</i> and a regex match group index <i>integer</i>.</p> 
        <p><b>Note:</b> The regex match group index starts from 1 and 0 means matching the whole regex. In addition, the regex match group index should not exceed the number of the defined groups.</p> 
        <p>E.g. <code>REGEXP_EXTRACT('foothebar', 'foo(.*?)(bar)', 2)"</code> returns "bar".</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
INITCAP(string)
{% endhighlight %}
      </td>
      <td>
        <p>Returns a new form of <i>string</i> with the first character of each word converted to uppercase and the rest characters to lowercase. Here a word means a sequences of alphanumeric characters.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
CONCAT(string1, string2,...)
{% endhighlight %}
      </td>
      <td>
        <p>Returns a string that concatenates <i>string1, string2, ...</i>. Returns NULL if any argument is NULL.</p>
        <p>E.g., <code>CONCAT('AA', 'BB', 'CC')</code> returns "AABBCC".</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
CONCAT_WS(string1, string2, string3,...)
{% endhighlight %}
      </td>
      <td>
        <p>Returns a string that concatenates <i>string2, string3, ...</i> with a separator <i>string1</i>. The separator is added between the strings to be concatenated. Returns NULL If <i>string1</i> is NULL. Compared with <code>CONCAT()</code>, <code>CONCAT_WS()</code> automatically skips NULL arguments.</p> 
        <p>E.g., <code>CONCAT_WS('~', 'AA', NULL, 'BB', '', 'CC')</code> returns "AA~BB~~CC".</p>
  </td>
    </tr>

        <tr>
      <td>
        {% highlight text %}
LPAD(string1, integer, string2)
{% endhighlight %}
      </td>
      <td>
        <p>Returns a new string from <i>string1</i> left-padded with <i>string2</i> to a length of <i>integer</i> characters. If the length of <i>string1</i> is shorter than <i>integer</i>, returns <i>string1</i> shortened to <i>integer</i> characters.</p> 
        <p>E.g., <code>LPAD('hi',4,'??')</code> returns "??hi"; <code>LPAD('hi',1,'??')</code> returns "h".</p>
      </td>
    </tr>
    
    <tr>
      <td>
        {% highlight text %}
RPAD(string1, integer, string2)
{% endhighlight %}
      </td>
      <td>
        <p>Returns a new string from <i>string1</i> right-padded with <i>string2</i> to a length of <i>integer</i> characters. If the length of <i>string1</i> is shorter than <i>integer</i>, returns <i>string1</i> shortened to <i>integer</i> characters.</p> 
        <p>E.g., <code>RPAD('hi',4,'??')</code> returns "hi??", <code>RPAD('hi',1,'??')</code> returns "h".</p>
      </td>
    </tr>
    
    <tr>
      <td>
        {% highlight text %}
FROM_BASE64(string)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the base64-decoded result from <i>string</i>; returns NULL if <i>string</i> is NULL.</p> 
        <p>E.g., <code>FROM_BASE64('aGVsbG8gd29ybGQ=')</code> returns "hello world".</p>
      </td>
    </tr>
        
    <tr>
      <td>
        {% highlight text %}
TO_BASE64(string)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the base64-encoded result from <i>string</i>; returns NULL if <i>string</i> is NULL.</p> 
        <p>E.g., <code>TO_BASE64('hello world')</code> returns "aGVsbG8gd29ybGQ=".</p>
      </td>
    </tr>
    
    <tr>
      <td>
        {% highlight text %}
ASCII(string)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the numeric value of the first character of <i>string</i>. Returns NULL if <i>string</i> is NULL.</p>
        <p>Only supported in blink planner.</p>
        <p>E.g., <code>ascii('abc')</code> returns 97, and <code>ascii(CAST(NULL AS VARCHAR))</code> returns NULL.</p>
      </td>
    </tr>
    
    <tr>
      <td>
        {% highlight text %}
CHR(integer)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the ASCII character having the binary equivalent to <i>integer</i>. If <i>integer</i> is larger than 255, we will get the modulus of <i>integer</i> divided by 255 first, and returns <i>CHR</i> of the modulus. Returns NULL if <i>integer</i> is NULL.</p>
        <p>Only supported in blink planner.</p>
        <p>E.g., <code>chr(97)</code> returns a, <code>chr(353)</code> returns a, and <code>ascii(CAST(NULL AS VARCHAR))</code> returns NULL.</p>
      </td>
    </tr>
    
    <tr>
      <td>
        {% highlight text %}
DECODE(binary, string)
{% endhighlight %}
      </td>
      <td>
        <p>Decodes the first argument into a String using the provided character set (one of 'US-ASCII', 'ISO-8859-1', 'UTF-8', 'UTF-16BE', 'UTF-16LE', 'UTF-16'). If either argument is null, the result will also be null.</p>
        <p>Only supported in blink planner.</p>
      </td>
    </tr>
    
    <tr>
      <td>
        {% highlight text %}
ENCODE(string1, string2)
{% endhighlight %}
      </td>
      <td>
        <p>Encodes the <i>string1</i> into a BINARY using the provided <i>string2</i> character set (one of 'US-ASCII', 'ISO-8859-1', 'UTF-8', 'UTF-16BE', 'UTF-16LE', 'UTF-16'). If either argument is null, the result will also be null.</p>
        <p>Only supported in blink planner.</p>
      </td>
    </tr>
    
    <tr>
      <td>
        {% highlight text %}
INSTR(string1, string2)
{% endhighlight %}
      </td>
      <td>
        Returns the position of the first occurrence of <i>string2</i> in <i>string1</i>. Returns NULL if any of arguments is NULL.</p>
        <p>Only supported in blink planner.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
LEFT(string, integer)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the leftmost <i>integer</i> characters from the <i>string</i>. Returns EMPTY String if <i>integer</i> is negative. Returns NULL if any argument is NULL.</p>
        <p>Only supported in blink planner.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
RIGHT(string, integer)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the rightmost <i>integer</i> characters from the <i>string</i>. Returns EMPTY String if <i>integer</i> is negative. Returns NULL if any argument is NULL.</p>
        <p>Only supported in blink planner.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
LOCATE(string1, string2[, integer])
{% endhighlight %}
      </td>
      <td>
        <p>Returns the position of the first occurrence of <i>string1</i> in <i>string2</i> after position <i>integer</i>. Returns 0 if not found. Returns NULL if any of arguments is NULL.</p>
        <p>Only supported in blink planner.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
PARSE_URL(string1, string2[, string3])
{% endhighlight %}
      </td>
      <td>
        <p>Returns the specified part from the URL. Valid values for <i>string2</i> include 'HOST', 'PATH', 'QUERY', 'REF', 'PROTOCOL', 'AUTHORITY', 'FILE', and 'USERINFO'. Returns NULL if any of arguments is NULL.</p>
        <p>E.g., <code>parse_url('http://facebook.com/path1/p.php?k1=v1&k2=v2#Ref1', 'HOST')</code>, returns 'facebook.com'.</p>
        <p>Also a value of a particular key in QUERY can be extracted by providing the key as the third argument <i>string3</i>.</p>
        <p>E.g., <code>parse_url('http://facebook.com/path1/p.php?k1=v1&k2=v2#Ref1', 'QUERY', 'k1')</code> returns 'v1'. </p>
        <p>Only supported in blink planner.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
REGEXP(string1, string2)
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if any (possibly empty) substring of <i>string1</i> matches the Java regular expression <i>string2</i>, otherwise FALSE. Returns NULL if any of arguments is NULL.</p>
        <p>Only supported in blink planner.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
REVERSE(string)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the reversed string. Returns NULL if <i>string</i> is NULL.</p>
        <p>Only supported in blink planner.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
SPLIT_INDEX(string1, string2, integer1)
{% endhighlight %}
      </td>
      <td>
        <p>Splits <i>string1</i> by the delimiter <i>string2</i>, returns the <i>integer</i>th (zero-based) string of the split strings. Returns NULL if <i>integer</i> is negative. Returns NULL if any of arguments is NULL.</p>
        <p>Only supported in blink planner.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
STR_TO_MAP(string1[, string2, string3]])
{% endhighlight %}
      </td>
      <td>
        <p>Returns a map after splitting the <i>string1</i> into key/value pairs using delimiters. <i>string2</i> is the pair delimiter, default is ','. And <i>string3</i> is the key-value delimiter, default is '='.</p>
        <p>Only supported in blink planner.</p>
      </td>
    </tr>
    
    <tr>
      <td>
        {% highlight text %}
SUBSTR(string[, integer1[, integer2]])
{% endhighlight %}
      </td>
      <td>
        <p>Returns a substring of string starting from position integer1 with length integer2 (to the end by default).</p>
        <p>Only supported in blink planner.</p>
      </td>
    </tr>
        
  </tbody>
</table>
</div>

<div data-lang="Java/Python" markdown="1">

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">String functions</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>

  <tbody>
    <tr>
      <td>
        {% highlight java %}
STRING1 + STRING2
{% endhighlight %}
      </td>
      <td>
        <p>Returns the concatenation of <i>STRING1</i> and <i>STRING2</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
STRING.charLength()
{% endhighlight %}
      </td>
      <td>
        <p>Returns the number of characters in <i>STRING</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
STRING.upperCase()
{% endhighlight %}
      </td>
      <td>
        <p>Returns <i>STRING</i> in uppercase.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
STRING.lowerCase()
{% endhighlight %}
      </td>
      <td>
        <p>Returns <i>STRING</i> in lowercase.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
STRING1.position(STRING2)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the position (start from 1) of the first occurrence of <i>STRING1</i> in <i>STRING2</i>; returns 0 if <i>STRING1</i> cannot be found in <i>STRING2</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
STRING1.trim(LEADING, STRING2)
STRING1.trim(TRAILING, STRING2)
STRING1.trim(BOTH, STRING2)
STRING1.trim(BOTH)
STRING1.trim()
{% endhighlight %}
      </td>
      <td>
        <p>Returns a string that removes leading and/or trailing characters <i>STRING2</i> from <i>STRING1</i>. By default, whitespaces at both sides are removed.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
STRING.ltrim()
{% endhighlight %}
      </td>
      <td>
        <p>Returns a string that removes the left whitespaces from <i>STRING</i>.</p> 
        <p>E.g., <code>' This is a test String.'.ltrim()</code> returns "This is a test String.".</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
STRING.rtrim()
{% endhighlight %}
      </td>
      <td>
        <p>Returns a string that removes the right whitespaces from <i>STRING</i>.</p> 
        <p>E.g., <code>'This is a test String. '.rtrim()</code> returns "This is a test String.".</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
STRING.repeat(INT)
{% endhighlight %}
      </td>
      <td>
        <p>Returns a string that repeats the base <i>STRING</i> <i>INT</i> times.</p> 
        <p>E.g., <code>'This is a test String.'.repeat(2)</code> returns "This is a test String.This is a test String.".</p>
      </td>
    </tr>    

    <tr>
      <td>
        {% highlight java %}
STRING1.regexpReplace(STRING2, STRING3)
{% endhighlight %}
      </td>
       <td>
         <p>Returns a string from <i>STRING1</i> with all the substrings that match a regular expression <i>STRING2</i> consecutively being replaced with <i>STRING3</i>.</p> 
         <p>E.g., <code>'foobar'.regexpReplace('oo|ar', '')</code> returns "fb".</p>
      </td>
    </tr>
    
    <tr>
      <td>
        {% highlight java %}
STRING1.overlay(STRING2, INT1)
STRING1.overlay(STRING2, INT1, INT2)
{% endhighlight %}
      </td>
      <td>
        <p>Returns a string that replaces <i>INT2</i> (<i>STRING2</i>'s length by default) characters of <i>STRING1</i> with <i>STRING2</i> from position <i>INT1</i>.</p>
        <p>E.g., <code>'xxxxxtest'.overlay('xxxx', 6)</code> returns "xxxxxxxxx"; <code>'xxxxxtest'.overlay('xxxx', 6, 2)</code> returns "xxxxxxxxxst".</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
STRING.substring(INT1)
STRING.substring(INT1, INT2)
{% endhighlight %}
      </td>
      <td>
        <p>Returns a substring of <i>STRING</i> starting from position <i>INT1</i> with length <i>INT2</i> (to the end by default).</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
STRING1.replace(STRING2, STRING3)
{% endhighlight %}
      </td>
      <td>
        <p>Returns a new string which replaces all the occurrences of <i>STRING2</i> with <i>STRING3</i> (non-overlapping) from <i>STRING1</i>.</p>
        <p>E.g., <code>'hello world'.replace('world', 'flink')</code> returns 'hello flink'; <code>'ababab'.replace('abab', 'z')</code> returns 'zab'.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
STRING1.regexpExtract(STRING2[, INTEGER1])
{% endhighlight %}
      </td>
      <td>
        <p>Returns a string from <i>STRING1</i> which extracted with a specified regular expression <i>STRING2</i> and a regex match group index <i>INTEGER1</i>.</p>
        <p><b>Note:</b> The regex match group index starts from 1 and 0 means matching the whole regex. In addition, the regex match group index should not exceed the number of the defined groups.</p> 
        <p>E.g., <code>'foothebar'.regexpExtract('foo(.*?)(bar)', 2)</code> returns "bar".</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
STRING.initCap()
{% endhighlight %}
      </td>
      <td>
        <p>Returns a new form of <i>STRING</i> with the first character of each word converted to uppercase and the rest characters to lowercase. Here a word means a sequences of alphanumeric characters.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
concat(STRING1, STRING2, ...)
{% endhighlight %}
      </td>
      <td>
        <p>Returns a string that concatenates <i>STRING1, STRING2, ...</i>. Returns NULL if any argument is NULL.</p>
        <p>E.g., <code>concat('AA', 'BB', 'CC')</code> returns "AABBCC".</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
concat_ws(STRING1, STRING2, STRING3, ...)
{% endhighlight %}
      </td>
      <td>
        <p>Returns a string that concatenates <i>STRING2, STRING3, ...</i> with a separator <i>STRING1</i>. The separator is added between the strings to be concatenated. Returns NULL If <i>STRING1</i> is NULL. Compared with <code>concat()</code>, <code>concat_ws()</code> automatically skips NULL arguments.</p> 
        <p>E.g., <code>concat_ws('~', 'AA', Null(STRING), 'BB', '', 'CC')</code> returns "AA~BB~~CC".</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
STRING1.lpad(INT, STRING2)
{% endhighlight %}
      </td>
      <td>
        <p>Returns a new string from <i>STRING1</i> left-padded with <i>STRING2</i> to a length of <i>INT</i> characters. If the length of <i>STRING1</i> is shorter than <i>INT</i>, returns <i>STRING1</i> shortened to <i>INT</i> characters.</p> 
        <p>E.g., <code>'hi'.lpad(4, '??')</code> returns "??hi";  <code>'hi'.lpad(1, '??')</code> returns "h".</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
STRING1.rpad(INT, STRING2)
{% endhighlight %}
      </td>
      <td>
        <p>Returns a new string from <i>STRING1</i> right-padded with <i>STRING2</i> to a length of <i>INT</i> characters. If the length of <i>STRING1</i> is shorter than <i>INT</i>, returns <i>STRING1</i> shortened to <i>INT</i> characters.</p> 
        <p>E.g., <code>'hi'.rpad(4, '??')</code> returns "hi??";  <code>'hi'.rpad(1, '??')</code> returns "h".</p>
      </td>
    </tr>
    
    <tr>
      <td>
        {% highlight java %}
STRING.fromBase64()
{% endhighlight %}
      </td>
      <td>
        <p>Returns the base64-decoded result from <i>STRING</i>; returns NULL if <i>STRING</i> is NULL.</p> 
        <p>E.g., <code>'aGVsbG8gd29ybGQ='.fromBase64()</code> returns "hello world".</p>
      </td>
    </tr>
    
    <tr>
      <td>
        {% highlight java %}
STRING.toBase64()
{% endhighlight %}
      </td>
      <td>
        <p>Returns the base64-encoded result from <i>STRING</i>; returns NULL if <i>STRING</i> is NULL.</p>
         <p>E.g., <code>'hello world'.toBase64()</code> returns "aGVsbG8gd29ybGQ=".</p>
      </td>
    </tr>
  </tbody>
</table>
</div>

<div data-lang="scala" markdown="1">

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">String functions</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>

  <tbody>
    <tr>
      <td>
        {% highlight scala %}
STRING1 + STRING2
{% endhighlight %}
      </td>
      <td>
        <p>Returns the concatenation of <i>STRING1</i> and <i>STRING2</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
STRING.charLength()
{% endhighlight %}
      </td>
      <td>
        <p>Returns the number of characters in <i>STRING</i>.</p>
      </td>
    </tr> 

    <tr>
      <td>
        {% highlight scala %}
STRING.upperCase()
{% endhighlight %}
      </td>
      <td>
        <p>Returns <i>STRING</i> in uppercase.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
STRING.lowerCase()
{% endhighlight %}
      </td>
      <td>
        <p>Returns <i>STRING</i> in lowercase.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
STRING1.position(STRING2)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the position (start from 1) of the first occurrence of <i>STRING1</i> in <i>STRING2</i>; returns 0 if <i>STRING1</i> cannot be found in <i>STRING2</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
STRING.trim(
  leading = true,
  trailing = true,
  character = " ")
{% endhighlight %}
      </td>
      <td>
        <p>Returns a string that removes leading and/or trailing characters from <i>STRING</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
STRING.ltrim()
{% endhighlight %}
      </td>
      <td>
        <p>Returns a string that removes the left whitespaces from <i>STRING</i>.</p> 
        <p>E.g., <code>" This is a test String.".ltrim()</code> returns "This is a test String.".</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
STRING.rtrim()
{% endhighlight %}
      </td>
      <td>
        <p>Returns a string that removes the right whitespaces from <i>STRING</i>.</p> 
        <p>E.g., <code>"This is a test String. ".rtrim()</code> returns "This is a test String.".</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
STRING.repeat(INT)
{% endhighlight %}
      </td>
      <td>
        <p>Returns a string that repeats the base <i>STRING</i> <i>INT</i> times.</p> 
        <p>E.g., <code>"This is a test String.".repeat(2)</code> returns "This is a test String.This is a test String.".</p>
      </td>
    </tr> 

    <tr>
      <td>
        {% highlight scala %}
STRING1.regexpReplace(STRING2, STRING3)
{% endhighlight %}
      </td>
       <td>
         <p>Returns a string from <i>STRING1</i> with all the substrings that match a regular expression <i>STRING2</i> consecutively being replaced with <i>STRING3</i>.</p> 
         <p>E.g. <code>"foobar".regexpReplace("oo|ar", "")</code> returns "fb".</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
STRING1.overlay(STRING2, INT1)
STRING1.overlay(STRING2, INT1, INT2)
{% endhighlight %}
      </td>
      <td>
        <p>Returns a string that replaces <i>INT2</i> (<i>STRING2</i>'s length by default) characters of <i>STRING1</i> with <i>STRING2</i> from position <i>INT1</i>.</p>
        <p>E.g., <code>"xxxxxtest".overlay("xxxx", 6)</code> returns "xxxxxxxxx"; <code>"xxxxxtest".overlay("xxxx", 6, 2)</code> returns "xxxxxxxxxst".</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
STRING.substring(INT1)
STRING.substring(INT1, INT2)
{% endhighlight %}
      </td>
      <td>
        <p>Returns a substring of <i>STRING</i> starting from position <i>INT1</i> with length <i>INT2</i> (to the end by default).</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
STRING1.replace(STRING2, STRING3)
{% endhighlight %}
      </td>
      <td>
        <p>Returns a new string which replaces all the occurrences of <i>STRING2</i> with <i>STRING3</i> (non-overlapping) from <i>STRING1</i>.</p>
        <p>E.g., <code>"hello world".replace("world", "flink")</code> returns "hello flink"; <code>"ababab".replace("abab", "z")</code> returns "zab".</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
STRING1.regexpExtract(STRING2[, INTEGER1])
{% endhighlight %}
      </td>
      <td>
        <p>Returns a string from <i>STRING1</i> which extracted with a specified regular expression <i>STRING2</i> and a regex match group index <i>INTEGER1</i>.</p>
        <p><b>Note:</b> The regex match group index starts from 1 and 0 means matching the whole regex. In addition, the regex match group index should not exceed the number of the defined groups.</p>
        <p>E.g. <code>"foothebar".regexpExtract("foo(.*?)(bar)", 2)"</code> returns "bar".</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
STRING.initCap()
{% endhighlight %}
      </td>
      <td>
        <p>Returns a new form of <i>STRING</i> with the first character of each word converted to uppercase and the rest characters to lowercase. Here a word means a sequences of alphanumeric characters.</p>
      </td>
    </tr>
    
    <tr>
      <td>
        {% highlight scala %}
concat(STRING1, STRING2, ...)
{% endhighlight %}
      </td>
      <td>
        <p>Returns a string that concatenates <i>STRING1, STRING2, ...</i>. Returns NULL if any argument is NULL.</p>
        <p>E.g., <code>concat("AA", "BB", "CC")</code> returns "AABBCC".</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
concat_ws(STRING1, STRING2, STRING3, ...)
{% endhighlight %}
      </td>
      <td>
        <p>Returns a string that concatenates <i>STRING2, STRING3, ...</i> with a separator <i>STRING1</i>. The separator is added between the strings to be concatenated. Returns NULL If <i>STRING1</i> is NULL. Compared with <code>concat()</code>, <code>concat_ws()</code> automatically skips NULL arguments.</p> 
        <p>E.g., <code>concat_ws("~", "AA", Null(Types.STRING), "BB", "", "CC")</code> returns "AA~BB~~CC".</p>
      </td>
    </tr>
    
    <tr>
      <td>
        {% highlight scala %}
STRING1.lpad(INT, STRING2)
{% endhighlight %}
      </td>
      <td>
        <p>Returns a new string from <i>STRING1</i> left-padded with <i>STRING2</i> to a length of <i>INT</i> characters. If the length of <i>STRING1</i> is shorter than <i>INT</i>, returns <i>STRING1</i> shortened to <i>INT</i> characters.</p> 
        <p>E.g., <code>"hi".lpad(4, "??")</code> returns "??hi";  <code>"hi".lpad(1, "??")</code> returns "h".</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
STRING1.rpad(INT, STRING2)
{% endhighlight %}
      </td>
      <td>
        <p>Returns a new string from <i>STRING1</i> right-padded with <i>STRING2</i> to a length of <i>INT</i> characters. If the length of <i>STRING1</i> is shorter than <i>INT</i>, returns <i>STRING1</i> shortened to <i>INT</i> characters.</p> 
        <p>E.g., <code>"hi".rpad(4, "??")</code> returns "hi??";  <code>"hi".rpad(1, "??")</code> returns "h".</p>
      </td>
    </tr>
    
    <tr>
      <td>
        {% highlight scala %}
STRING.fromBase64()
{% endhighlight %}
      </td>
      <td>
        <p>Returns the base64-decoded result from <i>STRING</i>; returns null If <i>STRING</i> is NULL.</p> 
        <p>E.g., <code>"aGVsbG8gd29ybGQ=".fromBase64()</code> returns "hello world".</p>
      </td>
    </tr>
    
    <tr>
      <td>
        {% highlight scala %}
STRING.toBase64()
{% endhighlight %}
      </td>
      <td>
        <p>Returns the base64-encoded result from <i>STRING</i>; returns NULL if <i>STRING</i> is NULL.</p>
         <p>E.g., <code>"hello world".toBase64()</code> returns "aGVsbG8gd29ybGQ=".</p>
      </td>
    </tr>
  </tbody>
</table>
</div>
</div>

{% top %}

### Temporal Functions

<div class="codetabs" markdown="1">
<div data-lang="SQL" markdown="1">
<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Temporal functions</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>

  <tbody>
    <tr>
      <td>
        {% highlight text %}
DATE string
{% endhighlight %}
      </td>
      <td>
        <p>Returns a SQL date parsed from <i>string</i> in form of "yyyy-MM-dd".</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
TIME string
{% endhighlight %}
      </td>
      <td>
        <p>Returns a SQL time parsed from <i>string</i> in form of "HH:mm:ss".</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
TIMESTAMP string
{% endhighlight %}
      </td>
      <td>
        <p>Returns a SQL timestamp parsed from <i>string</i> in form of "yyyy-MM-dd HH:mm:ss[.SSS]".</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
INTERVAL string range
{% endhighlight %}
      </td>
      <td>
        <p>Parses an interval <i>string</i> in the form "dd hh:mm:ss.fff" for SQL intervals of milliseconds or "yyyy-mm" for SQL intervals of months. An interval range might be <code>DAY</code>, <code>MINUTE</code>, <code>DAY TO HOUR</code>, or <code>DAY TO SECOND</code> for intervals of milliseconds; <code>YEAR</code> or <code>YEAR TO MONTH</code> for intervals of months.</p> 
        <p>E.g., <code>INTERVAL '10 00:00:00.004' DAY TO SECOND</code>, <code>INTERVAL '10' DAY</code>, or <code>INTERVAL '2-10' YEAR TO MONTH</code> return intervals.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
CURRENT_DATE
{% endhighlight %}
      </td>
      <td>
        <p>Returns the current SQL date in the UTC time zone.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
CURRENT_TIME
{% endhighlight %}
      </td>
      <td>
        <p>Returns the current SQL time in the UTC time zone.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
CURRENT_TIMESTAMP
{% endhighlight %}
      </td>
      <td>
        <p>Returns the current SQL timestamp in the UTC time zone.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
LOCALTIME
{% endhighlight %}
      </td>
      <td>
        <p>Returns the current SQL time in local time zone.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
LOCALTIMESTAMP
{% endhighlight %}
      </td>
      <td>
        <p>Returns the current SQL timestamp in local time zone.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
EXTRACT(timeintervalunit FROM temporal)
{% endhighlight %}
      </td>
      <td>
        <p>Returns a long value extracted from the <i>timeintervalunit</i> part of <i>temporal</i>.</p>
        <p>E.g., <code>EXTRACT(DAY FROM DATE '2006-06-05')</code> returns 5.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
YEAR(date)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the year from SQL date <i>date</i>. Equivalent to EXTRACT(YEAR FROM date).</p> 
        <p>E.g., <code>YEAR(DATE '1994-09-27')</code> returns 1994.</p>
      </td>
    </tr>
    
    <tr>
      <td>
        {% highlight text %}
QUARTER(date)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the quarter of a year (an integer between 1 and 4) from SQL date <i>date</i>. Equivalent to <code>EXTRACT(QUARTER FROM date)</code>.</p> 
        <p>E.g., <code>QUARTER(DATE '1994-09-27')</code> returns 3.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
MONTH(date)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the month of a year (an integer between 1 and 12) from SQL date <i>date</i>. Equivalent to <code>EXTRACT(MONTH FROM date)</code>.</p> 
        <p>E.g., <code>MONTH(DATE '1994-09-27')</code> returns 9.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
WEEK(date)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the week of a year (an integer between 1 and 53) from SQL date <i>date</i>. Equivalent to <code>EXTRACT(WEEK FROM date)</code>.</p>
        <p>E.g., <code>WEEK(DATE '1994-09-27')</code> returns 39.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
DAYOFYEAR(date)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the day of a year (an integer between 1 and 366) from SQL date <i>date</i>. Equivalent to <code>EXTRACT(DOY FROM date)</code>.</p>
        <p>E.g., <code>DAYOFYEAR(DATE '1994-09-27')</code> returns 270.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
DAYOFMONTH(date)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the day of a month (an integer between 1 and 31) from SQL date <i>date</i>. Equivalent to <code>EXTRACT(DAY FROM date)</code>.</p>
        <p>E.g., <code>DAYOFMONTH(DATE '1994-09-27')</code> returns 27.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
DAYOFWEEK(date)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the day of a week (an integer between 1 and 7; Sunday = 1) from SQL date <i>date</i>.Equivalent to <code>EXTRACT(DOW FROM date)</code>.</p>
        <p>E.g., <code>DAYOFWEEK(DATE '1994-09-27')</code> returns 3.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
HOUR(timestamp)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the hour of a day (an integer between 0 and 23) from SQL timestamp <i>timestamp</i>. Equivalent to <code>EXTRACT(HOUR FROM timestamp)</code>.</p>
        <p>E.g., <code>HOUR(TIMESTAMP '1994-09-27 13:14:15')</code> returns 13.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
MINUTE(timestamp)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the minute of an hour (an integer between 0 and 59) from SQL timestamp <i>timestamp</i>. Equivalent to <code>EXTRACT(MINUTE FROM timestamp)</code>.</p>
        <p>E.g., <code>MINUTE(TIMESTAMP '1994-09-27 13:14:15')</code> returns 14.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
SECOND(timestamp)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the second of a minute (an integer between 0 and 59) from SQL timestamp. Equivalent to <code>EXTRACT(SECOND FROM timestamp)</code>.</p>
        <p>E.g., <code>SECOND(TIMESTAMP '1994-09-27 13:14:15')</code> returns 15.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
FLOOR(timepoint TO timeintervalunit)
{% endhighlight %}
      </td>
      <td>
        <p>Returns a value that rounds <i>timepoint</i> down to the time unit <i>timeintervalunit</i>.</p> 
        <p>E.g., <code>FLOOR(TIME '12:44:31' TO MINUTE)</code> returns 12:44:00.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
CEIL(timepoint TO timeintervalunit)
{% endhighlight %}
      </td>
      <td>
        <p>Returns a value that rounds <i>timepoint</i> up to the time unit <i>timeintervalunit</i>.</p>
        <p>E.g., <code>CEIL(TIME '12:44:31' TO MINUTE)</code> returns 12:45:00.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
(timepoint1, temporal1) OVERLAPS (timepoint2, temporal2)
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if two time intervals defined by (<i>timepoint1</i>, <i>temporal1</i>) and (<i>timepoint2</i>, <i>temporal2</i>) overlap. The temporal values could be either a time point or a time interval.</p>
        <p>E.g., <code>(TIME '2:55:00', INTERVAL '1' HOUR) OVERLAPS (TIME '3:30:00', INTERVAL '2' HOUR)</code> returns TRUE; <code>(TIME '9:00:00', TIME '10:00:00') OVERLAPS (TIME '10:15:00', INTERVAL '3' HOUR)</code> returns FALSE.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
DATE_FORMAT(timestamp, string)
{% endhighlight %}
      </td>
      <td>
        <p><span class="label label-danger">Attention for old planner</span> This function has serious bugs and should not be used for now. Please implement a custom UDF instead or use EXTRACT as a workaround.</p>
        <p>For blink planner, this converts <i>timestamp</i> to a value of string in the format specified by the date format <i>string</i>. The format string is compatible with Java's <a href="https://docs.oracle.com/javase/8/docs/api/java/text/SimpleDateFormat.html">SimpleDateFormat</a>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
TIMESTAMPADD(timeintervalunit, interval, timepoint)
{% endhighlight %}
      </td>
      <td>
        <p>Returns a new time value that adds a (signed) integer interval to <i>timepoint</i>. The unit for <i>interval</i> is given by the unit argument, which should be one of the following values: <code>SECOND</code>, <code>MINUTE</code>, <code>HOUR</code>, <code>DAY</code>, <code>WEEK</code>, <code>MONTH</code>, <code>QUARTER</code>, or <code>YEAR</code>.</p> 
        <p>E.g., <code>TIMESTAMPADD(WEEK, 1, DATE '2003-01-02')</code> returns <code>2003-01-09</code>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
TIMESTAMPDIFF(timepointunit, timepoint1, timepoint2)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the (signed) number of <i>timepointunit</i> between <i>timepoint1</i> and <i>timepoint2</i>. The unit for the interval is given by the first argument, which should be one of the following values: <code>SECOND</code>, <code>MINUTE</code>, <code>HOUR</code>, <code>DAY</code>, <code>MONTH</code>, or <code>YEAR</code>. See also the <a href="#time-interval-and-point-unit-specifiers">Time Interval and Point Unit Specifiers table</a>.</p>
        <p>E.g., <code>TIMESTAMPDIFF(DAY, TIMESTAMP '2003-01-02 10:00:00', TIMESTAMP '2003-01-03 10:00:00')</code> leads to <code>1</code>.</p>
      </td>
    </tr>
    
    <tr>
      <td>
      {% highlight text %}
CONVERT_TZ(string1, string2, string3)
{% endhighlight %}
      </td>
      <td>
        <p>Converts a datetime <i>string1</i> (with default ISO timestamp format 'yyyy-MM-dd HH:mm:ss') from time zone <i>string2</i> to time zone <i>string3</i>. The format of time zone should be either an abbreviation such as "PST", a full name such as "America/Los_Angeles", or a custom ID such as "GMT-8:00".</p>
        <p>E.g., <code>CONVERT('1970-01-01 00:00:00', 'UTC', 'America/Los_Angeles')</code> returns '1969-12-31 16:00:00'.</p>
        <p>Only supported in blink planner.</p>
      </td>
    </tr>
        
    <tr>
      <td>
      {% highlight text %}
FROM_UNIXTIME(numeric[, string])
{% endhighlight %}
      </td>
      <td>
        <p>Returns a representation of the <i>numeric</i> argument as a value in <i>string</i> format (default is 'YYYY-MM-DD hh:mm:ss'). <i>numeric</i> is an internal timestamp value representing seconds since '1970-01-01 00:00:00' UTC, such as produced by the UNIX_TIMESTAMP() function. The return value is expressed in the session time zone (specified in TableConfig).</p>
        <p>E.g., <code>FROM_UNIXTIME(44)</code> returns '1970-01-01 09:00:44' if in UTC time zone, but returns '1970-01-01 09:00:44' if in 'Asia/Tokyo' time zone.</p>
        <p>Only supported in blink planner.</p>
      </td>
    </tr>
    
    <tr>
      <td>
      {% highlight text %}
UNIX_TIMESTAMP()
{% endhighlight %}
      </td>
      <td>
        <p>Gets current Unix timestamp in seconds. This function is not deterministic.</p>
        <p>Only supported in blink planner.</p>
      </td>
    </tr>
    
    <tr>
      <td>
      {% highlight text %}
UNIX_TIMESTAMP(string1[, string2])
{% endhighlight %}
      </td>
      <td>
        <p>Converts date time string <i>string1</i> in format <i>string2</i> (by default: yyyy-MM-dd HH:mm:ss if not specified) to Unix timestamp (in seconds), using the specified timezone in table config.</p>
        <p>Only supported in blink planner.</p>
      </td>
    </tr>
        
    <tr>
      <td>
      {% highlight text %}
TO_DATE(string1[, string2])
{% endhighlight %}
      </td>
      <td>
        <p>Converts a date string <i>string1</i> with format <i>string2</i> (by default 'yyyy-MM-dd') to a date.</p>
        <p>Only supported in blink planner.</p>
      </td>
    </tr> 
       
    <tr>
      <td>
      {% highlight text %}
TO_TIMESTAMP(string1[, string2])
{% endhighlight %}
      </td>
      <td>
        <p>Converts date time string <i>string1</i> with format <i>string2</i> (by default: 'yyyy-MM-dd HH:mm:ss') under the session time zone (specified by TableConfig) to a timestamp.</p>
        <p>Only supported in blink planner.</p>
      </td>
    </tr>
        
    <tr>
      <td>
      {% highlight text %}
NOW()
{% endhighlight %}
      </td>
      <td>
        <p>Returns the current SQL timestamp in the UTC time zone. This function is not deterministic.</p>
        <p>Only supported in blink planner.</p>
      </td>
    </tr>

  </tbody>
</table>
</div>

<div data-lang="Java/Python" markdown="1">
<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Temporal functions</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  
  <tbody>
   <tr>
      <td>
        {% highlight java %}
STRING.toDate()
{% endhighlight %}
      </td>
      <td>
        <p>Returns a SQL date parsed from <i>STRING</i> in form of "yyyy-MM-dd".</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
STRING.toTime()
{% endhighlight %}
      </td>
      <td>
        <p>Returns a SQL time parsed from <i>STRING</i> in form of "HH:mm:ss".</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
STRING.toTimestamp()
{% endhighlight %}
      </td>
      <td>
        <p>Returns a SQL timestamp parsed from <i>STRING</i> in form of "yyyy-MM-dd HH:mm:ss[.SSS]".</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
NUMERIC.year
NUMERIC.years
{% endhighlight %}
      </td>
      <td>
        <p>Creates an interval of months for <i>NUMERIC</i> years.</p>
      </td>
    </tr>
    <tr>
      <td>
        {% highlight java %}
NUMERIC.quarter
NUMERIC.quarters
{% endhighlight %}
      </td>
      <td>
        <p>Creates an interval of months for <i>NUMERIC</i> quarters.</p>
        <p>E.g., <code>2.quarters</code> returns 6.</p>
      </td>
    </tr>
    <tr>
      <td>
        {% highlight java %}
NUMERIC.month
NUMERIC.months
{% endhighlight %}
      </td>
      <td>
        <p>Creates an interval of <i>NUMERIC</i> months.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
NUMERIC.week
NUMERIC.weeks
{% endhighlight %}
      </td>
      <td>
        <p>Creates an interval of milliseconds for <i>NUMERIC</i> weeks.</p>
        <p>E.g., <code>2.weeks</code> returns 1209600000.</p>
      </td>
    </tr>
    
    <tr>
      <td>
        {% highlight java %}
NUMERIC.day
NUMERIC.days
{% endhighlight %}
      </td>
      <td>
        <p>Creates an interval of milliseconds for <i>NUMERIC</i> days.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
NUMERIC.hour
NUMERIC.hours
{% endhighlight %}
      </td>
      <td>
        <p>Creates an interval of milliseconds for <i>NUMERIC</i> hours.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
NUMERIC.minute
NUMERIC.minutes
{% endhighlight %}
      </td>
      <td>
        <p>Creates an interval of milliseconds for <i>NUMERIC</i> minutes.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
NUMERIC.second
NUMERIC.seconds
{% endhighlight %}
      </td>
      <td>
        <p>Creates an interval of milliseconds for <i>NUMERIC</i> seconds.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
NUMERIC.milli
NUMERIC.millis
{% endhighlight %}
      </td>
      <td>
        <p>Creates an interval of <i>NUMERIC</i> milliseconds.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
currentDate()
{% endhighlight %}
      </td>
      <td>
        <p>Returns the current SQL date in the UTC time zone.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
currentTime()
{% endhighlight %}
      </td>
      <td>
        <p>Returns the current SQL time in the UTC time zone.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
currentTimestamp()
{% endhighlight %}
      </td>
      <td>
        <p>Returns the current SQL timestamp in the UTC time zone.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
localTime()
{% endhighlight %}
      </td>
      <td>
        <p>Returns the current SQL time in local time zone.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
localTimestamp()
{% endhighlight %}
      </td>
      <td>
        <p>Returns the current SQL timestamp in local time zone.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
TEMPORAL.extract(TIMEINTERVALUNIT)
{% endhighlight %}
      </td>
      <td>
        <p>Returns a long value extracted from the <i>TIMEINTERVALUNIT</i> part of <i>temporal</i>.</p>
        <p>E.g., <code>'2006-06-05'.toDate.extract(DAY)</code> returns 5; <code>'2006-06-05'.toDate.extract(QUARTER)</code> returns 2.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
TIMEPOINT.floor(TIMEINTERVALUNIT)
{% endhighlight %}
      </td>
      <td>
        <p>Returns a value that rounds <i>TIMEPOINT</i> down to the time unit <i>TIMEINTERVALUNIT</i>.</p> 
        <p>E.g., <code>'12:44:31'.toDate.floor(MINUTE)</code> returns 12:44:00.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
TIMEPOINT.ceil(TIMEINTERVALUNIT)
{% endhighlight %}
      </td>
      <td>
        <p>Returns a value that rounds <i>TIMEPOINT</i> up to the time unit <i>TIMEINTERVALUNIT</i>.</p>
        <p>E.g., <code>'12:44:31'.toTime.floor(MINUTE)</code> returns 12:45:00.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
temporalOverlaps(TIMEPOINT1, TEMPORAL1, TIMEPOINT2, TEMPORAL2)
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if two time intervals defined by (<i>TIMEPOINT1</i>, <i>TEMPORAL1</i>) and (<i>TIMEPOINT2</i>, <i>TEMPORAL2</i>) overlap. The temporal values could be either a time point or a time interval.</p>
        <p>E.g., <code>temporalOverlaps('2:55:00'.toTime, 1.hour, '3:30:00'.toTime, 2.hour)</code> returns TRUE.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
dateFormat(TIMESTAMP, STRING)
{% endhighlight %}
      </td>
      <td>
        <p><span class="label label-danger">Attention</span> This function has serious bugs and should not be used for now. Please implement a custom UDF instead or use extract() as a workaround.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
timestampDiff(TIMEPOINTUNIT, TIMEPOINT1, TIMEPOINT2)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the (signed) number of <i>TIMEPOINTUNIT</i> between <i>TIMEPOINT1</i> and <i>TIMEPOINT2</i>. The unit for the interval is given by the first argument, which should be one of the following values: <code>SECOND</code>, <code>MINUTE</code>, <code>HOUR</code>, <code>DAY</code>, <code>MONTH</code>, or <code>YEAR</code>. See also the <a href="#time-interval-and-point-unit-specifiers">Time Interval and Point Unit Specifiers table</a>.</p>
        <p>E.g., <code>timestampDiff(DAY, '2003-01-02 10:00:00'.toTimestamp, '2003-01-03 10:00:00'.toTimestamp)</code> leads to <code>1</code>.</p>
      </td>
    </tr>

    </tbody>
</table>
</div>

<div data-lang="scala" markdown="1">
<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Temporal functions</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>

  <tbody>
    <tr>
      <td>
        {% highlight scala %}
STRING.toDate
{% endhighlight %}
      </td>
      <td>
        <p>Returns a SQL date parsed from <i>STRING</i> in form of "yyyy-MM-dd".</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
STRING.toTime
{% endhighlight %}
      </td>
      <td>
        <p>Returns a SQL time parsed from <i>STRING</i> in form of "HH:mm:ss".</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
STRING.toTimestamp
{% endhighlight %}
      </td>
      <td>
        <p>Returns a SQL timestamp parsed from <i>STRING</i> in form of "yyyy-MM-dd HH:mm:ss[.SSS]".</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
NUMERIC.year
NUMERIC.years
{% endhighlight %}
      </td>
      <td>
        <p>Creates an interval of months for <i>NUMERIC</i> years.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
NUMERIC.quarter
NUMERIC.quarters
{% endhighlight %}
      </td>
      <td>
        <p>Creates an interval of months for <i>NUMERIC</i> quarters.</p>
        <p>E.g., <code>2.quarters</code> returns 6.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
NUMERIC.month
NUMERIC.months
{% endhighlight %}
      </td>
      <td>
        <p>Creates an interval of <i>NUMERIC</i> months.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
NUMERIC.week
NUMERIC.weeks
{% endhighlight %}
      </td>
      <td>
        <p>Creates an interval of milliseconds for <i>NUMERIC</i> weeks.</p>
        <p>E.g., <code>2.weeks</code> returns 1209600000.</p>
      </td>
    </tr>
    
    <tr>
      <td>
        {% highlight scala %}
NUMERIC.day
NUMERIC.days
{% endhighlight %}
      </td>
      <td>
        <p>Creates an interval of milliseconds for <i>NUMERIC</i> days.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
NUMERIC.hour
NUMERIC.hours
{% endhighlight %}
      </td>
      <td>
        <p>Creates an interval of milliseconds for <i>NUMERIC</i> hours.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
NUMERIC.minute
NUMERIC.minutes
{% endhighlight %}
      </td>
      <td>
        <p>Creates an interval of milliseconds for <i>NUMERIC</i> minutes.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
NUMERIC.second
NUMERIC.seconds
{% endhighlight %}
      </td>
      <td>
        <p>Creates an interval of milliseconds for <i>NUMERIC</i> seconds.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
NUMERIC.milli
NUMERIC.millis
{% endhighlight %}
      </td>
      <td>
        <p>Creates an interval of <i>NUMERIC</i> milliseconds.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
currentDate()
{% endhighlight %}
      </td>
      <td>
        <p>Returns the current SQL date in the UTC time zone.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
currentTime()
{% endhighlight %}
      </td>
      <td>
        <p>Returns the current SQL time in the UTC time zone.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
currentTimestamp()
{% endhighlight %}
      </td>
      <td>
        <p>Returns the current SQL timestamp in the UTC time zone.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
localTime()
{% endhighlight %}
      </td>
      <td>
        <p>Returns the current SQL time in local time zone.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
localTimestamp()
{% endhighlight %}
      </td>
      <td>
        <p>Returns the current SQL timestamp in local time zone.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
TEMPORAL.extract(TIMEINTERVALUNIT)
{% endhighlight %}
      </td>
      <td>
        <p>Returns a long value extracted from the <i>TIMEINTERVALUNIT</i> part of <i>temporal</i>.</p>
        <p>E.g., <code>"2006-06-05".toDate.extract(TimeIntervalUnit.DAY)</code> returns 5; <code>"2006-06-05".toDate.extract(QUARTER)</code> returns 2.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
TIMEPOINT.floor(TIMEINTERVALUNIT)
{% endhighlight %}
      </td>
      <td>
        <p>Returns a value that rounds <i>TIMEPOINT</i> down to the time unit <i>TIMEINTERVALUNIT</i>.</p> 
        <p>E.g., <code>"12:44:31".toDate.floor(TimeIntervalUnit.MINUTE)</code> returns 12:44:00.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
TIMEPOINT.ceil(TIMEINTERVALUNIT)
{% endhighlight %}
      </td>
      <td>
        <p>Returns a value that rounds <i>TIMEPOINT</i> up to the time unit <i>TIMEINTERVALUNIT</i>.</p>
        <p>E.g., <code>"12:44:31".toTime.floor(TimeIntervalUnit.MINUTE)</code> returns 12:45:00.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
temporalOverlaps(TIMEPOINT1, TEMPORAL1, TIMEPOINT2, TEMPORAL2)
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if two time intervals defined by (<i>TIMEPOINT1</i>, <i>TEMPORAL1</i>) and (<i>TIMEPOINT2</i>, <i>TEMPORAL2</i>) overlap. The temporal values could be either a time point or a time interval.</p>
        <p>E.g., <code>temporalOverlaps("2:55:00".toTime, 1.hour, "3:30:00".toTime, 2.hour)</code> returns TRUE.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
dateFormat(TIMESTAMP, STRING)
{% endhighlight %}
      </td>
      <td>
        <p><span class="label label-danger">Attention</span> This function has serious bugs and should not be used for now. Please implement a custom UDF instead or use extract() as a workaround.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
timestampDiff(TIMEPOINTUNIT, TIMEPOINT1, TIMEPOINT2)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the (signed) number of <i>TIMEPOINTUNIT</i> between <i>TIMEPOINT1</i> and <i>TIMEPOINT2</i>. The unit for the interval is given by the first argument, which should be one of the following values: <code>SECOND</code>, <code>MINUTE</code>, <code>HOUR</code>, <code>DAY</code>, <code>MONTH</code>, or <code>YEAR</code>. See also the <a href="#time-interval-and-point-unit-specifiers">Time Interval and Point Unit Specifiers table</a>.</p>
        <p>E.g., <code>timestampDiff(TimePointUnit.DAY, '2003-01-02 10:00:00'.toTimestamp, '2003-01-03 10:00:00'.toTimestamp)</code> leads to <code>1</code>.</p>
      </td>
    </tr>

  </tbody>
</table>
</div>
</div>

{% top %}

### Conditional Functions

<div class="codetabs" markdown="1">

<div data-lang="SQL" markdown="1">
<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Conditional functions</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>

  <tbody>
    <tr>
      <td>
        {% highlight text %}
CASE value
WHEN value1_1 [, value1_2 ]* THEN result1
[ WHEN value2_1 [, value2_2 ]* THEN result2 ]*
[ ELSE resultZ ]
END
{% endhighlight %}
      </td>
      <td>
        <p>Returns <i>resultX</i> when the first time <i>value</i> is contained in (<i>valueX_1, valueX_2, ...</i>).
        When no value matches, returns <i>resultZ</i> if it is provided and returns NULL otherwise.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
CASE
WHEN condition1 THEN result1
[ WHEN condition2 THEN result2 ]*
[ ELSE resultZ ]
END
{% endhighlight %}
      </td>
      <td>
        <p>Returns <i>resultX</i> when the first <i>conditionX</i> is met. 
        When no condition is met, returns <i>resultZ</i> if it is provided and returns NULL otherwise.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
NULLIF(value1, value2)
{% endhighlight %}
      </td>
      <td>
        <p>Returns NULL if <i>value1</i> is equal to <i>value2</i>; returns <i>value1</i> otherwise.</p>
        <p>E.g., <code>NULLIF(5, 5)</code> returns NULL; <code>NULLIF(5, 0)</code> returns 5.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
COALESCE(value1, value2 [, value3 ]* )
{% endhighlight %}
      </td>
      <td>
        <p>Returns the first value that is not NULL from <i>value1, value2, ...</i>.</p>
        <p>E.g., <code>COALESCE(NULL, 5)</code> returns 5.</p>
      </td>
    </tr>
    
    <tr>
      <td>
        {% highlight text %}
IF(condition, true_value, false_value)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the <i>true_value</i> if <i>condition</i> is met, otherwise <i>false_value</i>.</p>
        <p>Only supported in blink planner.</p>
        <p>E.g., <code>IF(5 > 3, 5, 3)</code> returns 5.</p>
      </td>
    </tr>    

    <tr>
      <td>
        {% highlight text %}
IS_ALPHA(string)
{% endhighlight %}
      </td>
      <td>
        <p>Returns true if all characters in <i>string</i> are letter, otherwise false.</p>
        <p>Only supported in blink planner.</p>
      </td>
    </tr>    

    <tr>
      <td>
        {% highlight text %}
IS_DECIMAL(string)
{% endhighlight %}
      </td>
      <td>
        <p>Returns true if <i>string</i> can be parsed to a valid numeric, otherwise false.</p>
        <p>Only supported in blink planner.</p>
      </td>
    </tr>    

    <tr>
      <td>
        {% highlight text %}
IS_DIGIT(string)
{% endhighlight %}
      </td>
      <td>
        <p>Returns true if all characters in <i>string</i> are digit, otherwise false.</p>
        <p>Only supported in blink planner.</p>
      </td>
    </tr>
    
  </tbody>
</table>
</div>

<div data-lang="Java/Python" markdown="1">
<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Conditional functions</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  
  <tbody>
    <tr>
      <td>
        {% highlight java %}
BOOLEAN.?(VALUE1, VALUE2)
{% endhighlight %}
      </td>
      <td>
        <p>Returns <i>VALUE1</i> if <i>BOOLEAN</i> evaluates to TRUE; returns <i>VALUE2</i> otherwise.</p> 
        <p>E.g., <code>(42 > 5).?('A', 'B')</code> returns "A".</p>
      </td>
    </tr>
    </tbody>
</table>
</div>

<div data-lang="scala" markdown="1">
<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Conditional functions</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  
  <tbody>
    <tr>
      <td>
        {% highlight scala %}
BOOLEAN.?(VALUE1, VALUE2)
{% endhighlight %}
      </td>
      <td>
        <p>Returns <i>VALUE1</i> if <i>BOOLEAN</i> evaluates to TRUE; returns <i>VALUE2</i> otherwise.</p> 
        <p>E.g., <code>(42 > 5).?("A", "B")</code> returns "A".</p>
      </td>
    </tr>
    </tbody>
</table>
</div>
</div>

{% top %}

### Type Conversion Functions

<div class="codetabs" markdown="1">

<div data-lang="SQL" markdown="1">
<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Type conversion functions</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>

  <tbody>
    <tr>
      <td>
        {% highlight text %}
CAST(value AS type)
{% endhighlight %}
      </td>
      <td>
        <p>Returns a new <i>value</i> being cast to type <i>type</i>. See the supported types <a href="/dev/table/types.html">here</a>.</p>
        <p>E.g., <code>CAST('42' AS INT)</code> returns 42; <code>CAST(NULL AS VARCHAR)</code> returns NULL of type VARCHAR.</p>
      </td>
    </tr>
  </tbody>
</table>
</div>

<div data-lang="Java/Python" markdown="1">
<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Type conversion functions</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  
  <tbody>
    <tr>
      <td>
        {% highlight java %}
ANY.cast(TYPE)
{% endhighlight %}
      </td>
      <td>
        <p>Returns a new <i>ANY</i> being cast to type <i>TYPE</i>. See the supported types <a href="/dev/table/tableApi.html#data-types">here</a>.</p>
        <p>E.g., <code>'42'.cast(INT)</code> returns 42; <code>Null(STRING)</code> returns NULL of type STRING.</p>
      </td>
    </tr>
    </tbody>
</table>
</div>

<div data-lang="scala" markdown="1">
<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Type conversion functions</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>

  <tbody>
    <tr>
      <td>
        {% highlight scala %}
ANY.cast(TYPE)
{% endhighlight %}
      </td>
      <td>
        <p>Returns a new <i>ANY</i> being cast to type <i>TYPE</i>. See the supported types <a href="/dev/table/tableApi.html#data-types">here</a>.</p>
        <p>E.g., <code>"42".cast(Types.INT)</code> returns 42; <code>Null(Types.STRING)</code> returns NULL of type STRING.</p>
      </td>
    </tr>
  </tbody>
</table>

</div>
</div>

{% top %}

### Collection Functions

<div class="codetabs" markdown="1">

<div data-lang="SQL" markdown="1">
<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Collection functions</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>

  <tbody>
    <tr>
      <td>
        {% highlight text %}
CARDINALITY(array)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the number of elements in <i>array</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
array ‘[’ integer ‘]’
{% endhighlight %}
      </td>
      <td>
        <p>Returns the element at position <i>integer</i> in <i>array</i>. The index starts from 1.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
ELEMENT(array)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the sole element of <i>array</i> (whose cardinality should be one); returns NULL if <i>array</i> is empty. Throws an exception if <i>array</i> has more than one element.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
CARDINALITY(map)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the number of entries in <i>map</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
map ‘[’ value ‘]’
{% endhighlight %}
      </td>
      <td>
        <p>Returns the value specified by key <i>value</i> in <i>map</i>.</p>
      </td>
    </tr>
  </tbody>
</table>
</div>

<div data-lang="Java/Python" markdown="1">
<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Collection functions</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>

  <tbody>
    <tr>
      <td>
        {% highlight java %}
ARRAY.cardinality()
{% endhighlight %}
      </td>
      <td>
        <p>Returns the number of elements in <i>ARRAY</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
ARRAY.at(INT)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the element at position <i>INT</i> in <i>ARRAY</i>. The index starts from 1.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
ARRAY.element()
{% endhighlight %}
      </td>
      <td>
        <p>Returns the sole element of <i>ARRAY</i> (whose cardinality should be one); returns NULL if <i>ARRAY</i> is empty. Throws an exception if <i>ARRAY</i> has more than one element.</p>
      </td>
    </tr>
    
    <tr>
      <td>
        {% highlight java %}
MAP.cardinality()
{% endhighlight %}
      </td>
      <td>
        <p>Returns the number of entries in <i>MAP</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
MAP.at(ANY)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the value specified by key <i>ANY</i> in <i>MAP</i>.</p>
      </td>
    </tr>
  </tbody>
</table>
</div>

<div data-lang="scala" markdown="1">
<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Collection functions</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>

  <tbody>
    <tr>
      <td>
        {% highlight scala %}
ARRAY.cardinality()
{% endhighlight %}
      </td>
      <td>
        <p>Returns the number of elements in <i>ARRAY</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
ARRAY.at(INT)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the element at position <i>INT</i> in <i>ARRAY</i>. The index starts from 1.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
ARRAY.element()
{% endhighlight %}
      </td>
      <td>
        <p>Returns the sole element of <i>ARRAY</i> (whose cardinality should be one); returns NULL if <i>ARRAY</i> is empty. Throws an exception if <i>ARRAY</i> has more than one element.</p>
      </td>
    </tr>
    
    <tr>
      <td>
        {% highlight scala %}
MAP.cardinality()
{% endhighlight %}
      </td>
      <td>
        <p>Returns the number of entries in <i>MAP</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
MAP.at(ANY)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the value specified by key <i>ANY</i> in <i>MAP</i>.</p>
      </td>
    </tr>
  </tbody>
</table>
</div>
</div>


### Value Construction Functions

<div class="codetabs" markdown="1">
<div data-lang="SQL" markdown="1">
<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Value construction functions</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>

  <tbody>
    <tr>
      <td>
        {% highlight text %}
ROW(value1, [, value2]*)
(value1, [, value2]*)
{% endhighlight %}
      </td>
      <td>
        <p>Returns a row created from a list of values (<i>value1, value2,</i>...).</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
ARRAY ‘[’ value1 [, value2 ]* ‘]’
{% endhighlight %}
      </td>
      <td>
        <p>Returns an array created from a list of values (<i>value1, value2</i>, ...).</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
MAP ‘[’ value1, value2 [, value3, value4 ]* ‘]’
{% endhighlight %}
      </td>
      <td>
        <p>Returns a map created from a list of key-value pairs ((<i>value1, value2</i>), <i>(value3, value4)</i>, ...).</p>
      </td>
    </tr>
  </tbody>
</table>

</div>
<div data-lang="Java/Python" markdown="1">
<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Value constructor functions</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  
  <tbody>
    <tr>
      <td>
        {% highlight java %}
row(ANY1, ANY2, ...)
{% endhighlight %}
      </td>
      <td>
        <p>Returns a row created from a list of object values (<i>ANY1, ANY2</i>, ...). Row is composite type that can be access via <a href="#value-access-functions">value access functions</a>.</p>
      </td>
    </tr>
    
    <tr>
      <td>
        {% highlight java %}
array(ANY1, ANY2, ...)
{% endhighlight %}
      </td>
      <td>
        <p>Returns an array created from a list of object values (<i>ANY1, ANY2</i>, ...).</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
map(ANY1, ANY2, ANY3, ANY4, ...)
{% endhighlight %}
      </td>
      <td>
        <p>Returns a map created from a list of key-value pairs ((<i>ANY1, ANY2</i>), <i>(ANY3, ANY4)</i>, ...).</p>
      </td>
    </tr>
    
    <tr>
      <td>
        {% highlight java %}
NUMERIC.rows
{% endhighlight %}
      </td>
      <td>
        <p>Creates a <i>NUMERIC</i> interval of rows (commonly used in window creation).</p>
      </td>
    </tr>
    </tbody>
</table>

</div>
<div data-lang="scala" markdown="1">
<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Value constructor functions</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>

  <tbody>
    <tr>
      <td>
        {% highlight scala %}
row(ANY1, ANY2, ...)
{% endhighlight %}
      </td>
      <td>
        <p>Returns a row created from a list of object values (<i>ANY1, ANY2</i>, ...). Row is composite type that can be access via <a href="#value-access-functions">value access functions</a>.</p>
      </td>
    </tr>
    
    <tr>
      <td>
        {% highlight scala %}
array(ANY1, ANY2, ...)
{% endhighlight %}
      </td>
      <td>
        <p>Returns an array created from a list of object values (<i>ANY1, ANY2</i>, ...).</p>
      </td>
    </tr>
    
    <tr>
      <td>
        {% highlight scala %}
map(ANY1, ANY2, ANY3, ANY4, ...)
{% endhighlight %}
      </td>
      <td>
        <p>Returns a map created from a list of key-value pairs ((<i>ANY1, ANY2</i>), <i>(ANY3, ANY4)</i>, ...).</p>
      </td>
    </tr>
    
    <tr>
      <td>
        {% highlight scala %}
NUMERIC.rows
{% endhighlight %}
      </td>
      <td>
        <p>Creates a <i>NUMERIC</i> interval of rows (commonly used in window creation).</p>
      </td>
    </tr>
  </tbody>
</table>

</div>
</div>

{% top %}

### Value Access Functions

<div class="codetabs" markdown="1">
<div data-lang="SQL" markdown="1">
<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Value access functions</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>

  <tbody>
    <tr>
      <td>
        {% highlight text %}
tableName.compositeType.field
{% endhighlight %}
      </td>
      <td>
        <p>Returns the value of a field from a Flink composite type (e.g., Tuple, POJO) by name.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
tableName.compositeType.*
{% endhighlight %}
      </td>
      <td>
        <p>Returns a flat representation of a Flink composite type (e.g., Tuple, POJO) that converts each of its direct subtype into a separate field.
        In most cases the fields of the flat representation are named similarly to the original fields but with a dollar separator (e.g., <code>mypojo$mytuple$f0</code>).</p>
      </td>
    </tr>
  </tbody>
</table>
</div>

<div data-lang="Java/Python" markdown="1">
<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Value access functions</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>

  <tbody>
    <tr>
      <td>
        {% highlight java %}
COMPOSITE.get(STRING)
COMPOSITE.get(INT)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the value of a field from a Flink composite type (e.g., Tuple, POJO) by name or index.</p>
        <p>E.g., <code>pojo.get('myField')</code> or <code>tuple.get(0)</code>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
ANY.flatten()
{% endhighlight %}
      </td>
      <td>
        <p>Returns a flat representation of a Flink composite type (e.g., Tuple, POJO) that converts each of its direct subtype into a separate field.
         In most cases the fields of the flat representation are named similarly to the original fields but with a dollar separator (e.g., <code>mypojo$mytuple$f0</code>).</p>
      </td>
    </tr>
  </tbody>
</table>
</div>
<div data-lang="scala" markdown="1">
<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Value access functions</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>

  <tbody>
    <tr>
      <td>
        {% highlight scala %}
COMPOSITE.get(STRING)
COMPOSITE.get(INT)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the value of a field from a Flink composite type (e.g., Tuple, POJO) by name or index.</p>
        <p>E.g., <code>'pojo.get("myField")</code> or <code>'tuple.get(0)</code>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
ANY.flatten()
{% endhighlight %}
      </td>
      <td>
        <p>Returns a flat representation of a Flink composite type (e.g., Tuple, POJO) that converts each of its direct subtype into a separate field.
         In most cases the fields of the flat representation are named similarly to the original fields but with a dollar separator (e.g., <code>mypojo$mytuple$f0</code>).</p>
      </td>
    </tr>
  </tbody>
</table>

</div>
</div>

{% top %}

### Grouping Functions

<div class="codetabs" markdown="1">
<div data-lang="SQL" markdown="1">
<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Grouping functions</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>

  <tbody>
    <tr>
      <td>
        {% highlight text %}
GROUP_ID()
{% endhighlight %}
      </td>
      <td>
        <p>Returns an integer that uniquely identifies the combination of grouping keys.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
GROUPING(expression1 [, expression2]* )
GROUPING_ID(expression1 [, expression2]* )
{% endhighlight %}
      </td>
      <td>
        <p>Returns a bit vector of the given grouping expressions.</p>
      </td>
    </tr>
  </tbody>
</table>
</div>

<div data-lang="Java/Python" markdown="1">
<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Grouping functions</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  <tbody>
  </tbody>
</table>
</div>

<div data-lang="Scala" markdown="1">
<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Grouping functions</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  <tbody>
  </tbody>
</table>
</div>
</div>

### Hash Functions

<div class="codetabs" markdown="1">
<div data-lang="SQL" markdown="1">
<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Hash functions</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>

  <tbody>
    <tr>
      <td>
        {% highlight text %}
MD5(string)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the MD5 hash of <i>string</i> as a string of 32 hexadecimal digits; returns NULL if <i>string</i> is NULL.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
SHA1(string)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the SHA-1 hash of <i>string</i> as a string of 40 hexadecimal digits; returns NULL if <i>string</i> is NULL.</p>
      </td>
    </tr>
    
    <tr>
      <td>
        {% highlight text %}
SHA224(string)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the SHA-224 hash of <i>string</i> as a string of 56 hexadecimal digits; returns NULL if <i>string</i> is NULL.</p>
      </td>
    </tr>    
    
    <tr>
      <td>
        {% highlight text %}
SHA256(string)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the SHA-256 hash of <i>string</i> as a string of 64 hexadecimal digits; returns NULL if <i>string</i> is NULL.</p>
      </td>
    </tr>
    
    <tr>
      <td>
        {% highlight text %}
SHA384(string)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the SHA-384 hash of <i>string</i> as a string of 96 hexadecimal digits; returns NULL if <i>string</i> is NULL.</p>
      </td>
    </tr>  

    <tr>
      <td>
        {% highlight text %}
SHA512(string)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the SHA-512 hash of <i>string</i> as a string of 128 hexadecimal digits; returns NULL if <i>string</i> is NULL.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
SHA2(string, hashLength)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the hash using the SHA-2 family of hash functions (SHA-224, SHA-256, SHA-384, or SHA-512). The first argument <i>string</i> is the string to be hashed and the second argument <i>hashLength</i> is the bit length of the result (224, 256, 384, or 512). Returns NULL if <i>string</i> or <i>hashLength</i> is NULL.
        </p>
      </td>
    </tr>
  </tbody>
</table>
</div>

<div data-lang="Java/Python" markdown="1">
<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Hash functions</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  
  <tbody>
    <tr>
      <td>
        {% highlight java %}
STRING.md5()
{% endhighlight %}
      </td>
      <td>
        <p>Returns the MD5 hash of <i>STRING</i> as a string of 32 hexadecimal digits; returns NULL if <i>STRING</i> is NULL.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
STRING.sha1()
{% endhighlight %}
      </td>
      <td>
        <p>Returns the SHA-1 hash of <i>STRING</i> as a string of 40 hexadecimal digits; returns NULL if <i>STRING</i> is NULL.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
STRING.sha224()
{% endhighlight %}
      </td>
      <td>
        <p>Returns the SHA-224 hash of <i>STRING</i> as a string of 56 hexadecimal digits; returns NULL if <i>STRING</i> is NULL.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
STRING.sha256()
{% endhighlight %}
      </td>
      <td>
        <p>Returns the SHA-256 hash of <i>STRING</i> as a string of 64 hexadecimal digits; returns NULL if <i>STRING</i> is NULL.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
STRING.sha384()
{% endhighlight %}
      </td>
      <td>
        <p>Returns the SHA-384 hash of <i>STRING</i> as a string of 96 hexadecimal digits; returns NULL if <i>STRING</i> is NULL.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
STRING.sha512()
{% endhighlight %}
      </td>
      <td>
        <p>Returns the SHA-512 hash of <i>STRING</i> as a string of 128 hexadecimal digits; returns NULL if <i>STRING</i> is NULL.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
STRING.sha2(INT)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the SHA-2 family (SHA-224, SHA-256, SHA-384, or SHA-512) hashed value specified by <i>INT</i> (which could be 224, 256, 384, or 512) for <i>STRING</i>. Returns NULL if <i>STRING</i> or <i>INT</i> is NULL.
        </p>
      </td>
    </tr>
    </tbody>
</table>
</div>

<div data-lang="scala" markdown="1">
<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Hash functions</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  
  <tbody>
    <tr>
      <td>
        {% highlight scala %}
STRING.md5()
{% endhighlight %}
      </td>
      <td>
        <p>Returns the MD5 hash of <i>STRING</i> as a string of 32 hexadecimal digits; returns NULL if <i>STRING</i> is NULL.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
STRING.sha1()
{% endhighlight %}
      </td>
      <td>
        <p>Returns the SHA-1 hash of <i>STRING</i> as a string of 40 hexadecimal digits; returns NULL if <i>STRING</i> is NULL.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
STRING.sha224()
{% endhighlight %}
      </td>
      <td>
        <p>Returns the SHA-224 hash of <i>STRING</i> as a string of 56 hexadecimal digits; returns NULL if <i>STRING</i> is NULL.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
STRING.sha256()
{% endhighlight %}
      </td>
      <td>
        <p>Returns the SHA-256 hash of <i>STRING</i> as a string of 64 hexadecimal digits; returns NULL if <i>STRING</i> is NULL.</p>
      </td>
    </tr>
    
    <tr>
      <td>
        {% highlight scala %}
STRING.sha384()
{% endhighlight %}
      </td>
      <td>
        <p>Returns the SHA-384 hash of <i>STRING</i> as a string of 96 hexadecimal digits; returns NULL if <i>STRING</i> is NULL.</p>
      </td>
    </tr>    

    <tr>
      <td>
        {% highlight scala %}
STRING.sha512()
{% endhighlight %}
      </td>
      <td>
        <p>Returns the SHA-512 hash of <i>STRING</i> as a string of 128 hexadecimal digits; returns NULL if <i>STRING</i> is NULL.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
STRING.sha2(INT)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the SHA-2 family (SHA-224, SHA-256, SHA-384, or SHA-512) hashed value specified by <i>INT</i> (which could be 224, 256, 384, or 512) for <i>STRING</i>. Returns NULL if <i>STRING</i> or <i>INT</i> is NULL.
        </p>
      </td>
    </tr>
    </tbody>
</table>
</div>
</div>

{% top %}

### Auxiliary Functions

<div class="codetabs" markdown="1">

<div data-lang="SQL" markdown="1">
<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Auxiliary functions</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>

  <tbody>
  </tbody>
</table>
</div>

<div data-lang="Java/Python" markdown="1">
<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Auxiliary functions</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>

  <tbody>
    <tr>
      <td>
        {% highlight java %}
ANY.as(NAME1, NAME2, ...)
{% endhighlight %}
      </td>
      <td>
        <p>Specifies a name for <i>ANY</i> (a field). Additional names can be specified if the expression expands to multiple fields.</p>
      </td>
    </tr>
  </tbody>
</table>

</div>
<div data-lang="scala" markdown="1">

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Auxiliary functions</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>

  <tbody>
    <tr>
      <td>
        {% highlight scala %}
ANY.as(NAME1, NAME2, ...)
{% endhighlight %}
      </td>
      <td>
        <p>Specifies a name for <i>ANY</i> (a field). Additional names can be specified if the expression expands to multiple fields.</p>
      </td>
    </tr>
  </tbody>
</table>
</div>

</div>

Aggregate Functions
-------------------

The aggregate functions take an expression across all the rows as the input and return a single aggregated value as the result. 

<div class="codetabs" markdown="1">
<div data-lang="SQL" markdown="1">
<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Aggregate functions</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>

  <tbody>
    <tr>
      <td>
        {% highlight text %}
COUNT([ ALL ] expression | DISTINCT expression1 [, expression2]*)
{% endhighlight %}
      </td>
      <td>
        <p>By default or with ALL, returns the number of input rows for which <i>expression</i> is not NULL. Use DISTINCT for one unique instance of each value.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
COUNT(*)
COUNT(1)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the number of input rows.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
AVG([ ALL | DISTINCT ] expression)
{% endhighlight %}
      </td>
      <td>
        <p>By default or with keyword ALL, returns the average (arithmetic mean) of <i>expression</i> across all input rows. Use DISTINCT for one unique instance of each value.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
SUM([ ALL | DISTINCT ] expression)
{% endhighlight %}
      </td>
      <td>
        <p>By default or with keyword ALL, returns the sum of <i>expression</i> across all input rows. Use DISTINCT for one unique instance of each value.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
MAX([ ALL | DISTINCT ] expression)
{% endhighlight %}
      </td>
      <td>
        <p>By default or with keyword ALL, returns the maximum value of <i>expression</i> across all input rows. Use DISTINCT for one unique instance of each value.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
MIN([ ALL | DISTINCT ] expression)
{% endhighlight %}
      </td>
      <td>
        <p>By default or with keyword ALL, returns the minimum value of <i>expression</i> across all input rows. Use DISTINCT for one unique instance of each value.</p>
      </td>
    </tr>
    <tr>
      <td>
        {% highlight text %}
STDDEV_POP([ ALL | DISTINCT ] expression)
{% endhighlight %}
      </td>
      <td>
        <p>By default or with keyword ALL, returns the population standard deviation of <i>expression</i> across all input rows. Use DISTINCT for one unique instance of each value.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
STDDEV_SAMP([ ALL | DISTINCT ] expression)
{% endhighlight %}
      </td>
      <td>
        <p>By default or with keyword ALL, returns the sample standard deviation of <i>expression</i> across all input rows. Use DISTINCT for one unique instance of each value.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
VAR_POP([ ALL | DISTINCT ] expression)
{% endhighlight %}
      </td>
      <td>
        <p>By default or with keyword ALL, returns the population variance (square of the population standard deviation) of <i>expression</i> across all input rows. Use DISTINCT for one unique instance of each value.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
VAR_SAMP([ ALL | DISTINCT ] expression)
{% endhighlight %}
      </td>
      <td>
        <p>By default or with keyword ALL, returns the sample variance (square of the sample standard deviation) of <i>expression</i> across all input rows. Use DISTINCT for one unique instance of each value.</p>
      </td>
    </tr>

    <tr>
      <td>
          {% highlight text %}
COLLECT([ ALL | DISTINCT ] expression)
{% endhighlight %}
      </td>
      <td>
          <p>By default or with keyword ALL, returns a multiset of <i>expression</i> across all input rows. NULL values will be ignored. Use DISTINCT for one unique instance of each value.</p>
      </td>
    </tr>
    
    <tr>
      <td>
        {% highlight text %}
VARIANCE([ ALL | DISTINCT ] expression)
{% endhighlight %}
      </td>
      <td>
        <p>Synonyms for VAR_SAMP().</p>
        <p>Only supported in blink planner.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
RANK()
{% endhighlight %}
      </td>
      <td>
        <p>Returns the rank of a value in a group of values. The result is one plus the number of rows preceding or equal to the current row in the ordering of the partition. The values will produce gaps in the sequence.</p>
        <p>Only supported in blink planner.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
DENSE_RANK()
{% endhighlight %}
      </td>
      <td>
        <p>Returns the rank of a value in a group of values. The result is one plus the previously assigned rank value. Unlike the function rank, dense_rank will not produce gaps in the ranking sequence.</p>
        <p>Only supported in blink planner.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
ROW_NUMBER()
{% endhighlight %}
      </td>
      <td>
        <p>Assigns a unique, sequential number to each row, starting with one, according to the ordering of rows within the window partition.</p>
        <p>ROW_NUMBER and RANK are similar. ROW_NUMBER numbers all rows sequentially (for example 1, 2, 3, 4, 5). RANK provides the same numeric value for ties (for example 1, 2, 2, 4, 5).</p>
        <p>Only supported in blink planner.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
LEAD(expression [, offset] [, default] )
{% endhighlight %}
      </td>
      <td>
        <p>Returns the value of <i>expression</i> at the <i>offset</i>th row after the current row in the window. The default value of <i>offset</i> is 1 and the default value of <i>default</i> is NULL.</p>
        <p>Only supported in blink planner.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
LAG(expression [, offset] [, default])
{% endhighlight %}
      </td>
      <td>
        <p>Returns the value of <i>expression</i> at the <i>offset</i>th row after the current row in the window. The default value of <i>offset</i> is 1 and the default value of <i>default</i> is NULL.</p>
        <p>Only supported in blink planner.</p>
      </td>
    </tr>
        
    <tr>
      <td>
        {% highlight text %}
FIRST_VALUE(expression)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the first value in an ordered set of values.</p>
        <p>Only supported in blink planner.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
LAST_VALUE(expression)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the last value in an ordered set of values.</p>
        <p>Only supported in blink planner.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
LISTAGG(expression [, separator])
{% endhighlight %}
      </td>
      <td>
        <p>Concatenates the values of string expressions and places separator values between them. The separator is not added at the end of string. The default value of <i>separator</i> is ','.</p>
        <p>Only supported in blink planner.</p>
      </td>
    </tr>
           
  </tbody>
</table>

</div>

<div data-lang="Java/Python" markdown="1">
<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Aggregate functions</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  
  <tbody>
    <tr>
      <td>
        {% highlight java %}
FIELD.count
{% endhighlight %}
      </td>
      <td>
        <p>Returns the number of input rows for which <i>FIELD</i> is not NULL.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
FIELD.avg
{% endhighlight %}
      </td>
      <td>
        <p>Returns the average (arithmetic mean) of <i>FIELD</i> across all input rows.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
FIELD.sum
{% endhighlight %}
      </td>
      <td>
        <p>Returns the sum of numeric field <i>FIELD</i> across all input rows. If all values are NULL, returns NULL.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
FIELD.sum0
{% endhighlight %}
      </td>
      <td>
        <p>Returns the sum of numeric field <i>FIELD</i> across all input rows. If all values are NULL, returns 0.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
FIELD.max
{% endhighlight %}
      </td>
      <td>
        <p>Returns the maximum value of numeric field <i>FIELD</i> across all input rows.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
FIELD.min
{% endhighlight %}
      </td>
      <td>
        <p>Returns the minimum value of numeric field <i>FIELD</i> across all input rows.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
FIELD.stddevPop
{% endhighlight %}
      </td>
      <td>
        <p>Returns the population standard deviation of numeric field <i>FIELD</i> across all input rows.</p>
      </td>
    </tr>
    
    <tr>
      <td>
        {% highlight java %}
FIELD.stddevSamp
{% endhighlight %}
      </td>
      <td>
        <p>Returns the sample standard deviation of numeric field <i>FIELD</i> across all input rows.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
FIELD.varPop
{% endhighlight %}
      </td>
      <td>
        <p>Returns the population variance (square of the population standard deviation) of numeric field <i>FIELD</i> across all input rows.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
FIELD.varSamp
{% endhighlight %}
      </td>
      <td>
        <p>Returns the sample variance (square of the sample standard deviation) of numeric field <i>FIELD</i> across all input rows.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
FIELD.collect
{% endhighlight %}
      </td>
      <td>
        <p>Returns a multiset of <i>FIELD</i> across all input rows.</p>
      </td>
    </tr>
    </tbody>
</table>
</div>

<div data-lang="scala" markdown="1">
<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Aggregate functions</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>

  <tbody>
   <tr>
      <td>
        {% highlight scala %}
FIELD.count
{% endhighlight %}
      </td>
      <td>
        <p>Returns the number of input rows for which <i>FIELD</i> is not NULL.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
FIELD.avg
{% endhighlight %}
      </td>
      <td>
        <p>Returns the average (arithmetic mean) of <i>FIELD</i> across all input rows.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
FIELD.sum
{% endhighlight %}
      </td>
      <td>
        <p>Returns the sum of numeric field <i>FIELD</i> across all input rows. If all values are NULL, returns NULL.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
FIELD.sum0
{% endhighlight %}
      </td>
      <td>
        <p>Returns the sum of numeric field <i>FIELD</i> across all input rows. If all values are NULL, returns 0.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
FIELD.max
{% endhighlight %}
      </td>
      <td>
        <p>Returns the maximum value of numeric field <i>FIELD</i> across all input rows.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
FIELD.min
{% endhighlight %}
      </td>
      <td>
        <p>Returns the minimum value of numeric field <i>FIELD</i> across all input rows.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
FIELD.stddevPop
{% endhighlight %}
      </td>
      <td>
        <p>Returns the population standard deviation of numeric field <i>FIELD</i> across all input rows.</p>
      </td>
    </tr>
    
    <tr>
      <td>
        {% highlight scala %}
FIELD.stddevSamp
{% endhighlight %}
      </td>
      <td>
        <p>Returns the sample standard deviation of numeric field <i>FIELD</i> across all input rows.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
FIELD.varPop
{% endhighlight %}
      </td>
      <td>
        <p>Returns the population variance (square of the population standard deviation) of numeric field <i>FIELD</i> across all input rows.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
FIELD.varSamp
{% endhighlight %}
      </td>
      <td>
        <p>Returns the sample variance (square of the sample standard deviation) of numeric field <i>FIELD</i> across all input rows.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
FIELD.collect
{% endhighlight %}
      </td>
      <td>
        <p>Returns a multiset of <i>FIELD</i> across all input rows.</p>
      </td>
    </tr>
  </tbody>
</table>
</div>
</div>

{% top %}

Time Interval and Point Unit Specifiers
---------------------------------------

The following table lists specifiers for time interval and time point units. 

For Table API, please use `_` for spaces (e.g., `DAY_TO_HOUR`).

| Time Interval Unit       | Time Point Unit                |
| :----------------------- | :----------------------------- |
| `MILLENIUM` _(SQL-only)_ |                                |
| `CENTURY` _(SQL-only)_   |                                |
| `YEAR`                   | `YEAR`                         |
| `YEAR TO MONTH`          |                                |
| `QUARTER`                | `QUARTER`                      |
| `MONTH`                  | `MONTH`                        |
| `WEEK`                   | `WEEK`                         |
| `DAY`                    | `DAY`                          |
| `DAY TO HOUR`            |                                |
| `DAY TO MINUTE`          |                                |
| `DAY TO SECOND`          |                                |
| `HOUR`                   | `HOUR`                         |
| `HOUR TO MINUTE`         |                                |
| `HOUR TO SECOND`         |                                |
| `MINUTE`                 | `MINUTE`                       |
| `MINUTE TO SECOND`       |                                |
| `SECOND`                 | `SECOND`                       |
|                          | `MILLISECOND`                  |
|                          | `MICROSECOND`                  |
| `DOY` _(SQL-only)_       |                                |
| `DOW` _(SQL-only)_       |                                |
|                          | `SQL_TSI_YEAR` _(SQL-only)_    |
|                          | `SQL_TSI_QUARTER` _(SQL-only)_ |
|                          | `SQL_TSI_MONTH` _(SQL-only)_   |
|                          | `SQL_TSI_WEEK` _(SQL-only)_    |
|                          | `SQL_TSI_DAY` _(SQL-only)_     |
|                          | `SQL_TSI_HOUR` _(SQL-only)_    |
|                          | `SQL_TSI_MINUTE` _(SQL-only)_  |
|                          | `SQL_TSI_SECOND ` _(SQL-only)_ |

{% top %}

Column Functions
---------------------------------------

The column functions are used to select or deselect table columns.

| SYNTAX              | DESC                         |
| :--------------------- | :-------------------------- |
| withColumns(...)         | select the specified columns                  |
| withoutColumns(...)        | deselect the columns specified                  |

The detailed syntax is as follows:

{% highlight text %}
columnFunction:
    withColumns(columnExprs)
    withoutColumns(columnExprs)

columnExprs:
    columnExpr [, columnExpr]*

columnExpr:
    columnRef | columnIndex to columnIndex | columnName to columnName

columnRef:
    columnName(The field name that exists in the table) | columnIndex(a positive integer starting from 1)
{% endhighlight %}

The usage of the column function is illustrated in the following table. (Suppose we have a table with 5 columns: `(a: Int, b: Long, c: String, d:String, e: String)`):


<div class="codetabs" markdown="1">
<div data-lang="Java/Python" markdown="1">

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">Api</th>
      <th class="text-center">Usage</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>
        withColumns(*)|*
      </td>
      <td>
{% highlight java %}
select("withColumns(*)") | select("*") = select("a, b, c, d, e")
{% endhighlight %}
      </td>
      <td>
        all the columns
      </td>
    </tr>
    
    <tr>
      <td>
        withColumns(m to n)
      </td>
      <td>
{% highlight java %}
select("withColumns(2 to 4)") = select("b, c, d")
{% endhighlight %}
      </td>
      <td>
        columns from m to n
      </td>
    </tr>
    
    <tr>
      <td>
        withColumns(m, n, k)
      </td>
      <td>
{% highlight java %}
select("withColumns(1, 3, e)") = select("a, c, e")
{% endhighlight %}
      </td>
      <td>
        columns m, n, k
      </td>
    </tr>
    
    <tr>
      <td>
        withColumns(m, n to k)
      </td>
      <td>
{% highlight java %}
select("withColumns(1, 3 to 5)") = select("a, c, d ,e")
{% endhighlight %}
      </td>
      <td>
        mixing of the above two representation
      </td>
    </tr>
    
    <tr>
      <td>
        withoutColumns(m to n)
      </td>
      <td>
{% highlight java %}
select("withoutColumns(2 to 4)") = select("a, e")
{% endhighlight %}
      </td>
      <td>
        deselect columns from m to n
      </td>
    </tr>

    <tr>
      <td>
        withoutColumns(m, n, k)
      </td>
      <td>
{% highlight java %}
select("withoutColumns(1, 3, 5)") = select("b, d")
{% endhighlight %}
      </td>
      <td>
        deselect columns m, n, k
      </td>
    </tr>
    
    <tr>
      <td>
        withoutColumns(m, n to k)
      </td>
      <td>
{% highlight java %}
select("withoutColumns(1, 3 to 5)") = select("b")
{% endhighlight %}
      </td>
      <td>
        mixing of the above two representation
      </td>
    </tr>
    
  </tbody>
</table>
</div>

<div data-lang="scala" markdown="1">
<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">Api</th>
      <th class="text-center">Usage</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>
        withColumns(*)|*
      </td>
      <td>
{% highlight scala %}
select(withColumns('*)) | select('*) = select('a, 'b, 'c, 'd, 'e)
{% endhighlight %}
      </td>
      <td>
        all the columns
      </td>
    </tr>
    
    <tr>
      <td>
        withColumns(m to n)
      </td>
      <td>
{% highlight scala %}
select(withColumns(2 to 4)) = select('b, 'c, 'd)
{% endhighlight %}
      </td>
      <td>
        columns from m to n
      </td>
    </tr>

    <tr>
      <td>
        withColumns(m, n, k)
      </td>
      <td>
{% highlight scala %}
select(withColumns(1, 3, 'e)) = select('a, 'c, 'e)
{% endhighlight %}
      </td>
      <td>
        columns m, n, k
      </td>
    </tr>

    <tr>
      <td>
        withColumns(m, n to k)
      </td>
      <td>
{% highlight scala %}
select(withColumns(1, 3 to 5)) = select('a, 'c, 'd, 'e)
{% endhighlight %}
      </td>
      <td>
        mixing of the above two representation
      </td>
    </tr>

    <tr>
      <td>
        withoutColumns(m to n)
      </td>
      <td>
{% highlight scala %}
select(withoutColumns(2 to 4)) = select('a, 'e)
{% endhighlight %}
      </td>
      <td>
        deselect columns from m to n
      </td>
    </tr>   
     
    <tr>
      <td>
        withoutColumns(m, n, k)
      </td>
      <td>
{% highlight scala %}
select(withoutColumns(1, 3, 5)) = select('b, 'd)
{% endhighlight %}
      </td>
      <td>
        deselect columns m, n, k
      </td>
    </tr>
   
    <tr>
      <td>
        withoutColumns(m, n to k)
      </td>
      <td>
{% highlight scala %}
select(withoutColumns(1, 3 to 5)) = select('b)
{% endhighlight %}
      </td>
      <td>
        mixing of the above two representation
      </td>
    </tr> 
    
  </tbody>
</table>
</div>
</div>

The column functions can be used in all places where column fields are expected, such as `select, groupBy, orderBy, UDFs etc.` e.g.:


<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
table
   .groupBy("withColumns(1 to 3)")
   .select("withColumns(a to b), myUDAgg(myUDF(withColumns(5 to 20)))")
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
table
   .groupBy(withColumns(1 to 3))
   .select(withColumns('a to 'b), myUDAgg(myUDF(withColumns(5 to 20))))
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
{% highlight python %}
table \
    .group_by("withColumns(1 to 3)") \
    .select("withColumns(a to b), myUDAgg(myUDF(withColumns(5 to 20)))")
{% endhighlight %}
</div>
</div>

<span class="label label-info">Note</span> Column functions are only used in Table API.

{% top %}
