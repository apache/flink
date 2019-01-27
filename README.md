# Blink 

As the internal version of Apache Flink, project Blink was initially launched at Alibaba to accommodate our use cases. The Blink team at Alibaba has made significant efforts to optimize the performance and improve the stability of Apache Flink. After careful consideration, we decide to open source Blink and donate it to the Apache Flink community. We hope our experience of Blink will help more Flink users who are facing the same issues we have seen in the past years. Being a part of the Apache Flink community, we are more than happy to work together with the community to contribute some of the features in Blink back to Flink.

This Blink release is based on Flink 1.5.1, with some additional features and bug fixes from later Flink versions. Moreover, there are considerable amount of new features, performance optimizations and stability improvements. The following sections introduce noticeable changes in this release compared with Apache Flink 1.5.1.

## API Compatibility
* DataStream and DataSet are mostly compatible with Flink 1.5.1. More specifically, only the following interfaces are enhanced to enable batch processing in stream operators.
OneInputStreamOperator, TwoInputStreamOperator, StreamOperator, RuntimeContext
* Table API and SQL have some incompatible changes
* UDF, UDTF, UDAF are not compatible in terms of the return types.

## Runtime
* Architecture
    * Re-designed the scheduler to support customized scheduling strategy for different processing mode (e.g. batch, streaming).
    * Introduced pluggable shuffle architecture to support customized shuffle service, which facilitates the adaptation to new processing mode or new hardware.
    * Full-stack fine-grained resource configuration.
* Efficiency
    * Introduced Operator DAG in replacement of OperatorChain to reduce data transfer cost.
    * Used ZeroCopy for pipeline shuffle to reduce memory footprint in network layer.
    * Avoided unnecessary serde cost in broadcast shuffle.
* Stability
    * Added new JM failover mechanism to avoid restart the entire job in case JM fails.
* Ecosystem and feature
    * Added native support for Kubernetes (beta version) with full elasticity of resources usage.
    * Added YARN based External Shuffle Service to release resource on job finish.
    * Added sorted Map State support.

## SQL
Blink has done significant amount of refactoring and optimization in SQL layer, including type system refactoring, raw binary format unification, etc. Meanwhile, we have changed the execution stack of Table and SQL. Instead of translate them to DataStream and DataSet, Blink Table API and SQL build DAG directly. Consequently, currently Blink users can no longer convert between Table and DataSet. However, users can still convert between Table and DataStream. The major features and optimizations shared by both stream and batch processing are following:

* Introduced DDL, namely CREATE TABLE. Users can define constraints such as primary key and unique key. It also support computed columns as well as watermark column.
* Support for multiple sinks. In case a SQL script has multiple INSERT INTO statement, Blink will try to compile them into a single DAG. Thus the same subgraph could be shared to reduce execution cost.
* Cherry-picked SQL client from later version of Apache Flink.
* Support both global, per operator type (available for some operator types), and per operator instance resource configuration.
* Added Decimal support, allow customized precision and scale.
* Added implicit type cast
* Added dozens of optimization rules along with various raw and derived table statistics for cost based optimizer.
* Built-in support for Parquet and Orc format

In addition to the above common improvements, there are quite a few changes we made to streaming and batch SQL respectively.
### Streaming SQL
Blink Streaming SQL bears quite a few works Alibaba has done to support internal online use cases. Some of the major changes include:
* Added Join with dimension tables.
* Added MiniBatch execution mode to reduce state IO. Users can set MiniBatch size and latency target. Flink will dynamically change batching to meet both requirements.
* Optimized state for inner join to achieve better performance, especially for stream-stream joins.
* Added TopN support
* Handled data skew. Blink effectively avoided the data skew caused by aggregate, especially in cases of DISTINCT operation.

### Batch SQL
Blink Batch SQL also has many important features and optimizations, including:
* All Join type support, including inner, left, right, full, semi and anti.
* Support multiple join implementations, including hash join, sort merge join, nested loop join
* Support sort aggregate and hash aggregate
* Support various OVER window syntax
* Support various subquery syntax, including in, exists.
* Support tumbling and sliding window.
* Support various advanced analytic operator, including cube, rollup, grouping set, etc.
* Support compression for data spilled to disk.
* Support Runtime Filter, which uses bloom filter to boost the query.
* Reorder the joins based on statistics.
* Remove unnecessary shuffles and sorts in optimization process.
* Support all TPC-H and TPC-DS Queries.

## Table API
Table API is a super set of SQL in terms of functionality. Besides, we have also introduced some important new features. One example is following:
* Added cache() API to support Interactive programming. Users can explicitly cache intermediate table result for later usage to avoid unnecessary duplicate computation. The feature is only available for Batch job at this point.

We are in the process of adding more useful features to Table API. Some of them have already been brought to the community for discussion. Keep tuned.

## Catalog
Blink has made the following changes and optimizations to the catalog.
* Unified the internal and external catalog with ReadableCatalog and ReadableWritableCatalog.
* Integration with Hive catalog. A new HiveCatalog class is introduced to read Hive metadata including databases, tables, table partitions, simple data types as well as table and column stats.
* Redefined the reference target domains, i.e. ‘mycatalog.mydatabase.mytable’. The reference level could be simplified to ‘mytable’ provided users have specified the default catalog and default database.

In the future, we plan to add support for more types of metadata and catalog.

## Hive Compatibility
Our goal is to make Flink fully compatible with Hive in both metadata and data layer. In this release,
* Flink can read metadata from Hive through the aforementioned HiveCatalog.
* Flink jobs can read from Hive tables and partitioned tables. It also supports partition pruning for the partitioned tables.

In the future, we will put more efforts to improve the compatibility with Hive, including supporting Hive specific data type and Hive UDF, etc.

## Zeppelin for Flink
In order to provide a better visualization and interaction with Flink, we have done significant work to Zeppelin to better support Flink. Some of these changes are in Flink, while some others are in Zeppelin. Before all these changes are merged back to Flink and Zeppelin, we would like to encourage users to try out this new experience by using the Zeppline image introduced in docs/quickstart/zeppelin_quickstart.md. The new features added to Zeppelin include:
* Support Flink Job submission in three modes: Local, Remote and YARN.
* Support Table API and SQL
* Support query to both static and dynamic tables.
* Auto association to Flink job URLs
* Support job cancelation and resumption with savepoint.
* Advanced features of ZeppelinContext in Flink Interpereter, such as creating widgets.
* Three built-in Flink tutorials: Streaming ETL, Flink Batch Tutorial, Flink Stream Tutorial.

## Flink Web
We have also made improvements to the usability and performance of Flink Runtime Web. Lots of new functions, such as resource utilization reporting, job performance tuning and log query, have been added to facilitate the operation to Flink jobs.
* Resource utilization monitoring
    * added resource information at three levels including Cluster, TaskManager and Job. Users can easily see the resource utilization.
* Job performance tuning: 
    * Added operator level topology and and data flow tracing. 
    * Added new metrics (InQueue and OutQueue sizes, etc) to track the backpressure, filter and data skew.
* Log query
    * Added Log association to Job, Vertex and SubTasks.
    * Added Multiple log access entrance point
    * Added log pagination and highlights
* Interaction improvements
    * Overall improvements to the user experience to avoid unnecessary page redirections.
* Performance improvement
    * Refactored the entire module to use Angular 7.0 and improved the performance by 1x

# Blink
Blink最初是阿里巴巴内部的Flink版本代号，基于阿里的场景做了大量的优化和稳定性改造工作。在经过一些讨论之后，我们决定将Blink的所有代码捐赠给Flink社区，并将其开源从而成为Flink社区的一部分。我们希望Blink的代码和经验能够帮助更多使用Flink的人，解决一些我们已经碰到过的问题。最终我们也希望和社区一起讨论和努力，将Blink中比较好的一些特性反馈回社区，为繁荣社区也出上我们一份力。

这次开源的Blink版本，主要是基于flink 1.5.1，也从社区的后续版本pick了一些比较有用的新特性和bugfix。除此之外，我们也加入了大量的新功能，在性能和稳定性上也做了很多优化。接下来我们会按模块来简单介绍下一些重要的新特性和改动。

## API兼容性
* DataStream和DataSet API基本和Flink 1.5.1版本兼容，只有OneInputStreamOperator、TwoInputStreamOperator、StreamOperator、RuntimeContext接口有变化，主要原因是为了使stream operator具有运行batch作业的能力
* TableAPI和SQL有部分不兼容的修改
* UDF、UDTF、UDAF的接口定义移到api包目录下，同时在涉及到返回类型的API上不兼容

## Runtime
为了解决阿里巴巴生产场景中遇到的各种挑战，Blink对Runtime架构、效率、稳定性方面都做了大量改进：
* 架构
    * Blink设计了新的调度架构，开发者可以根据计算模型自己的特点定制不同调度器；
    * 新的计算模型或者新的硬件都需要不同Shuffle进行适配，Blink引入的Pluggable Shuffle Architecture，方便用户对Flink Shuffle 生态进行扩展；
    * Blink Runtime的全栈上都支持用户对资源的精确控制和匹配。
* 效率
    * Blink引入了Operator DAG机制，替换了原有的OperatorChain，避免了不必要的数据传输开销；
    * Pipeline Shuffle经过ZeroCopy的改造之后，网络层内存消耗大量减少；
    * 优化BroadCast Shuffle中大量不必要的序列化和反序列化开销；
* 稳定性
    * 提供了新的JM FailOver机制，大大减少了JMFailOver对JOB的影响——JM发生错误之后，重新接管整个JOB而不是重启JOB
* 生态和功能
    * 原生支持Kubernetes（实验功能），不同于Standalone模式在K8s上的拉起，新功能在FLIP-6提出的架构上实现原生的融合，根据job的资源需求动态的申请/释放Pod来运行TaskExecutor，实现资源弹性提升资源的利用率
    * 实现了基于Yarn的External Shuffle Service，可以让任务执行完成之后及时归还资源
    * 增加了sorted map state

## SQL
我们在SQL层进行了大量的重构和优化，包括类型系统重构，基础数据结构的升级统一。同时我们也做了技术架构上的调整，SQL和TableAPI的程序最终执行的时候将不会翻译到DataStream和DataSet这两个API上，而是直接构建可运行的DAG。因此这个版本的SQL和TableAPI不能和DataSet这个API进行互相转换，但保留了和DataStream API互相转换的能力（将DataStream注册成表，或将Table转成DataStream后继续操作）。一些主要的并且流和批共享的新功能和优化如下：
* 加入了DDL的支持，主要是CREATE TABLE语法，支持primary key，unique key等constraint，同时还支持计算列和watermark
* 加入了多Sink的支持，在SQL中如果同时有多个insert into语句，我们会尝试将多个sink编译在一个DAG中，并且将中间部分进行复用（最典型的比如source）减小代价
* 从社区较新版本pick了SQL CLI的功能，方便用户体验SQL的一些基本功能
* 基于配置项来设置并发和资源，同时也支持最细粒度到算子级别的资源配置
* 增加了对Decimal的支持，可自定义precision和scale
* 增加了隐式转换的支持
* 增加了数十个优化规则，以及多种统计信息的收集和推导，帮助我们基于代价的优化器选择更优的plan
* 精确控制所有算子使用的内存，最大限度的避免运行时OOM的产生
* 内置支持Parquet和Orc两种文件格式

接下来将分为streaming和batch分别介绍各自特有的一些优化和实现：

### Streaming SQL
Streaming SQL部分积累了我们内部线上业务过去一两年间所做的大量新特性和优化，主要包括：
* 维表Join支持，通过继承LookupableTableSource接口的source即可被作为维表使用
* MiniBatch执行模式，在aggregate、join等需要和state频繁交互的算子中，我们加入了基于小batch来执行的一种模式。用户可以配置一个batch的大小同时控制端到端的延迟，我们会基于这两个因素来动态影响batch的策略
* InnerJoin的state优化，我们针对常用的双流inner join进行了大量的和state相关的性能优化
* TopN支持，我们会识别用户基于over window以及rank来实现类似topn需求的执行计划，并将其优化为一种高效的实现
* Aggregate数据倾斜处理，我们增加了2阶段改写的功能，能够有效避免aggregate尤其是涉及到distinct后聚合时容易造成数据倾斜的问题

### Batch SQL
Batch SQL也是我们优化和实现新feature的一个重点，主要包括：
* 支持所有join的类型，包括inner、left、right、full。同时也包括semi和anti join
* 支持hash join，sort merge join，nestedloop join等实现策略
* 支持sort aggregate和hash aggregate
* 支持多种over window语法
* 支持多种sub query的写法比如in，exits等，并且会生成比较高效的执行计划
* 支持tumbling和sliding window
* 支持多种高级分析语法，如cube、rollup、grouping set等
* 算子spill数据加入了压缩的支持
* 支持Runtime Filter，可以在join之前使用bloom filter过滤大量无用的数据
* 支持基于统计信息的join reorder
* 支持在优化过程中移除不必要的shuffle和排序
* 支持所有TPCH和TPCDS的query

## TableAPI
TableAPI在功能上是SQL的超集，因此上面提到的新增加的stream/batch SQL的功能，我们在tableAPI也添加了相对应的API。除此之外，我们还在TableAPI上引入了一些新的功能。这里我们列举一个比较重要的功能。
* 为了增强interactive programming体验，我们添加了cache功能。有了这个功能之后用户可以根据需要来cache计算的中间结果，从而避免不必要的重复计算。这个功能目前只对batch job有效

后续我们会在tableAPI上添加更多有用的功能。很多新功能已经在社区展开讨论。

## Catalog
在catalog上做了如下修改和优化：
* 通过引入全新的 ReadableCatalog and ReadableWritableCatalog 接口统一了 Flink 的内部和外部 catalog。Flink 所有的 catalog 会被 TableEnvironment 中的 CatalogManager管理。
* 实现了两种新的 catalog - FlinkInMemoryCatalog and HiveCatalog。FlinkInMemoryCatalog 会将所有元数据存在内存中。HiveCatalog 会连接 Hive metastore 并桥接 Flink 和 Hive 之间的元数据。目前，这个HiveCatalog 可以提供读取 Hive 元数据的能力，包括数据库（databases），表（tables），表分区（table partitions）, 简单的数据类型（simple data types）, 表和列的统计信息（table and column stats）。
* 重新清晰定义了引用目标的层级，即 'mycatalog.mydatabase.mytable'。通过定义默认 catalog 和默认数据库，用户可以将引用层级简单化为 'mytable’。

未来，我们还将加入对更多类型的元数据以及catalog的支持。

## Hive兼容性
我们的目标是在元数据(meta data)和数据层将 Flink 和 Hive 对接和打通。
* 在这个版本上，Flink可以通过上面提到的HiveCatalog读取Hive的metaData。
* 这个版本实现了HiveTableSource，使得Flink job可以直接读取Hive中普通表和分区表的数据，以及做分区的裁剪。

通过这个版本，用户可以使用Flink SQL读取已有的Hive meta和data，做数据处理。未来我们将在Flink上继续加大对Hive兼容性的支持，包括支持Hive特有的data type，和Hive UDF等等。

## Zeppelin for Flink
为了提供更好的可视化和交互式体验，我们做了大量的工作让Zeppelin能够更好的支持Flink。这些改动有些是在Flink上的，有些是在Zeppelin上的。在全部推回Flink和Zeppelin社区之前，可以使用这个Zeppelin image(具体细节请参考docs/quickstart/zeppelin_quickstart.md)来测试和使用这些功能。新添加的Zeppelin功能包括：
* 支持3种运行模式提交Flink job：Local, Remote和Yarn
* 支持运行tableAPI和文本SQL
* 支持对静态table和动态table的查询
* 能够自动关联Job URL
* 支持cancel Job, 支持resume job with savepoint
* 在Flink Interpreter里可以使用ZeppelinContext的高级功能，比如创建控件
* 提供3个built-in的Flink tutorial的例子: Streaming ETL, Flink Batch Tutorial, Flink Stream Tutorial

## Flink Web
我们在 Flink Runtime Web 的易用性与性能等多个方面进行了改进，从资源使用、作业调优、日志查询等维度新增了大量功能，使得用户可以更方便的对 Flink 作业进行运维。
* 资源使用：新增 Cluster、TaskManager 与 Job 三个级别的资源信息，资源的申请与使用情况一目了然。
* 作业调优：作业的拓扑关系及数据流向可以追溯至 Operator 级别，Vertex 增加 InQueue，OutQueue等多项指标，可以方便的追踪数据的反压、过滤及倾斜情况。
* 日志查询：TaskManager 和 JobManager 的日志功能得到大幅度加强，从Job、Vertex、SubTask 等多个维度都可以关联至对应日志，提供多日志文件访问入口，以及分页展示查询和日志高亮功能。
* 交互优化：对页面交互逻辑进行了整体优化，绝大部分关联信息在单个页面就可以完成查询和比对工作，减少了大量不必要的跳转。
* 性能提升：使用 Angular 7.0 进行了整体重构，页面运行性能有了一倍以上的提升。在大数据量情况下也不会发生页面假死或者卡顿情况。
