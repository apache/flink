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

它支持在PyFlink表和Pandas DataFrame之间进行转换。 

* This will be replaced by the TOC
{:toc}

## 将Pandas DataFrame转换为PyFlink表

它支持从Pandas DataFrame创建PyFlink表。在内部，它将在客户端使用
Arrow列格式对Pandas DataFrame进行序列化，并且序列化的数据将
在执行期间在Arrow源中进行处理和反序列化。 Arrow源还可以用于流作业中，
它将正确处理检查点并提供恰好一次的保证。 

以下示例显示如何从Pandas DataFrame创建PyFlink表： 

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

## 将PyFlink表转换为Pandas DataFrame 

它还支持将PyFlink表转换为Pandas DataFrame。在内部，它将具体化表的结果，并
将其在客户端序列化为Arrow列格式的多个Arrow批次。最大Arrow批处理大小
由配置选项[python.fn-execution.arrow.batch.size]({{ site.baseurl }}/dev/table/python/python_config.html#python-fn-execution-arrow-batch-size) 确定。然
后，序列化的数据将转换为Pandas DataFrame。 

以下示例显示了如何将PyFlink表转换为Pandas DataFrame： 

{% highlight python %}
import pandas as pd
import numpy as np

# Create a PyFlink Table
pdf = pd.DataFrame(np.random.rand(1000, 2))
table = t_env.from_pandas(pdf, ["a", "b"]).filter("a > 0.5")

# Convert the PyFlink Table to a Pandas DataFrame
pdf = table.to_pandas()
{% endhighlight %}
