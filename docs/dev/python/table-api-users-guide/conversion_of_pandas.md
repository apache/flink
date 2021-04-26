---
title: "Conversions between PyFlink Table and Pandas DataFrame"
nav-parent_id: python_tableapi
nav-pos: 40
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

PyFlink Table API supports conversion to and from Pandas DataFrame.

* This will be replaced by the TOC
{:toc}

## Convert Pandas DataFrame to PyFlink Table

Pandas DataFrames can be converted into a PyFlink Table.
Internally, PyFlink will serialize the Pandas DataFrame using Arrow columnar format on the client. 
The serialized data will be processed and deserialized in Arrow source during execution. 
The Arrow source can also be used in streaming jobs, and is integrated with checkpointing to
provide exactly-once guarantees.

The following example shows how to create a PyFlink Table from a Pandas DataFrame:

{% highlight python %}
import pandas as pd
import numpy as np

# Create a Pandas DataFrame
pdf = pd.DataFrame(np.random.rand(1000, 2))

# Create a PyFlink Table from a Pandas DataFrame
table = t_env.from_pandas(pdf)

# Create a PyFlink Table from a Pandas DataFrame with the specified column names
table = t_env.from_pandas(pdf, ['f0', 'f1'])

# Create a PyFlink Table from a Pandas DataFrame with the specified column types
table = t_env.from_pandas(pdf, [DataTypes.DOUBLE(), DataTypes.DOUBLE()])

# Create a PyFlink Table from a Pandas DataFrame with the specified row type
table = t_env.from_pandas(pdf,
                          DataTypes.ROW([DataTypes.FIELD("f0", DataTypes.DOUBLE()),
                                         DataTypes.FIELD("f1", DataTypes.DOUBLE())])
{% endhighlight %}

## Convert PyFlink Table to Pandas DataFrame

PyFlink Tables can additionally be converted into a Pandas DataFrame.
The resulting rows will be serialized as multiple Arrow batches of Arrow columnar format on the client. 
The maximum Arrow batch size is configured via the option [python.fn-execution.arrow.batch.size]({% link dev/python/table-api-users-guide/python_config.md %}#python-fn-execution-arrow-batch-size).
The serialized data will then be converted to a Pandas DataFrame. 
Because the contents of the table will be collected on the client, please ensure that the results of the table can fit in memory before calling this method.
You can limit the number of rows collected to client side via <a href="{{ site.pythondocs_baseurl }}/api/python/pyflink.table.html#pyflink.table.Table.limit">Table.limit</a>

The following example shows how to convert a PyFlink Table to a Pandas DataFrame:

{% highlight python %}
import pandas as pd
import numpy as np

# Create a PyFlink Table
pdf = pd.DataFrame(np.random.rand(1000, 2))
table = t_env.from_pandas(pdf, ["a", "b"]).filter(col('a') > 0.5)

# Convert the PyFlink Table to a Pandas DataFrame
pdf = table.limit(100).to_pandas()
{% endhighlight %}
