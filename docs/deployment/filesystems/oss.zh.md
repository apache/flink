---
title: "阿里云对象存储服务 (OSS)"
nav-title: 阿里云 OSS
nav-parent_id: filesystems
nav-pos: 2
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

## OSS：对象存储服务

[阿里云对象存储服务](https://www.aliyun.com/product/oss) (Aliyun OSS) 使用广泛，尤其在中国云用户中十分流行，能提供多种应用场景下的云对象存储。OSS 可与 Flink 一起使用以读取与存储数据，以及与[流 State Backend]({% link ops/state/state_backends.zh.md %}) 结合使用。

* This will be replaced by the TOC
{:toc}

通过以下格式指定路径，OSS 对象可类似于普通文件使用：

{% highlight plain %}
oss://<your-bucket>/<object-name>
{% endhighlight %}

以下代码展示了如何在 Flink 作业中使用 OSS：

{% highlight java %}
// 读取 OSS bucket
env.readTextFile("oss://<your-bucket>/<object-name>");

// 写入 OSS bucket
stream.writeAsText("oss://<your-bucket>/<object-name>")

// 将 OSS 用作 FsStatebackend
env.setStateBackend(new FsStateBackend("oss://<your-bucket>/<object-name>"));
{% endhighlight %}

### Shaded Hadoop OSS 文件系统

为使用 `flink-oss-fs-hadoop`，在启动 Flink 之前，将对应的 JAR 文件从 `opt` 目录复制到 Flink 发行版中的 `plugin` 目录下的一个文件夹中，例如：

{% highlight bash %}
mkdir ./plugins/oss-fs-hadoop
cp ./opt/flink-oss-fs-hadoop-{{ site.version }}.jar ./plugins/oss-fs-hadoop/
{% endhighlight %}

`flink-oss-fs-hadoop` 为使用 *oss://* scheme 的 URI 注册了默认的文件系统包装器。

#### 配置设置

在设置好 OSS 文件系统包装器之后，需要添加一些配置以保证 Flink 有权限访问 OSS buckets。

为了简单使用，可直接在 `flink-conf.yaml` 中使用与 Hadoop `core-site.xml` 相同的配置关键字。

可在 [Hadoop OSS 文档](http://hadoop.apache.org/docs/current/hadoop-aliyun/tools/hadoop-aliyun/index.html) 中查看配置关键字。

一些配置必须添加至 `flink-conf.yaml` （**在 Hadoop OSS 文档中定义的其它配置为用作性能调优的高级配置**）：

{% highlight yaml %}
fs.oss.endpoint: 连接的 Aliyun OSS endpoint
fs.oss.accessKeyId: Aliyun access key ID
fs.oss.accessKeySecret: Aliyun access key secret
{% endhighlight %}

备选的 `CredentialsProvider` 也可在 `flink-conf.yaml` 中配置，例如：
{% highlight yaml %}
# 从 OSS_ACCESS_KEY_ID 和 OSS_ACCESS_KEY_SECRET 读取凭据 (Credentials)
fs.oss.credentials.provider: com.aliyun.oss.common.auth.EnvironmentVariableCredentialsProvider
{% endhighlight %}

其余的凭据提供者（credential providers）可在[这里](https://github.com/aliyun/aliyun-oss-java-sdk/tree/master/src/main/java/com/aliyun/oss/common/auth)中找到。



{% top %}
