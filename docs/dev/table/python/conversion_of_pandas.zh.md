---
title: "PyFlink Table 和 Pandas DataFrame 互转"
nav-parent_id: python_tableapi
nav-pos: 50
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

It supports to convert between PyFlink Table and Pandas DataFrame.

* This will be replaced by the TOC
{:toc}

## Convert Pandas DataFrame to PyFlink Table

It supports to create a PyFlink Table from a Pandas DataFrame. Internally, it will serialize the Pandas DataFrame
using Arrow columnar format at client side and the serialized data will be processed and deserialized in Arrow source
during execution. The Arrow source could also be used in streaming jobs and it will properly handle the checkpoint
and provides the exactly once guarantees.

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
