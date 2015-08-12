# flink-storm-compatibility

The Storm compatibility layer allows to embed spouts or bolt unmodified within a regular Flink streaming program (`StormSpoutWrapper` and `StormBoltWrapper`). Additionally, a whole Storm topology can be submitted to Flink (see `FlinkTopologyBuilder`, `FlinkLocalCluster`, and `FlinkSubmitter`). Only a few minor changes to the original submitting code are required. The code that builds the topology itself, can be reused unmodified. See `flink-storm-examples` for a simple word-count example.

The following Strom features are not (yet/fully) supported by the compatibility layer right now:
* the spout/bolt configuration within `open()`/`prepare()` is not yet supported (ie, `Map conf` parameter)
* topology and tuple meta information (ie, `TopologyContext` not fully supported)
* no fault-tolerance guarantees (ie, calls to `ack()`/`fail()` and anchoring is ignored)
* for whole Storm topologies the following is not supported by Flink:
  * direct emit connection pattern
  * activating/deactivating and rebalancing of topologies
  * task hooks
  * custom metrics
