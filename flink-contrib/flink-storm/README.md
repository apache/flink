# flink-storm

`flink-storm` is compatibility layer for Apache Storm and allows to embed Spouts or Bolts unmodified within a regular Flink streaming program (`SpoutWrapper` and `BoltWrapper`).
Additionally, a whole Storm topology can be submitted to Flink (see `FlinkTopologyBuilder`, `FlinkLocalCluster`, and `FlinkSubmitter`).
Only a few minor changes to the original submitting code are required.
The code that builds the topology itself, can be reused unmodified. See `flink-storm-examples` for a simple word-count example.

The following Storm features are not (yet/fully) supported by the compatibility layer right now:
* tuple meta information
* no fault-tolerance guarantees (ie, calls to `ack()`/`fail()` and anchoring is ignored)
* for whole Storm topologies the following is not supported by Flink:
  * direct emit connection pattern
  * activating/deactivating and rebalancing of topologies
  * task hooks
  * metrics
