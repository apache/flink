# flink-storm-examples

This module contains multiple versions of a simple word-count-example to illustrate the usage of the compatibility layer:
* the usage of spouts or bolt within a regular Flink streaming program (ie, embedded spouts or bolts)
   1. `SpoutSourceWordCount` uses a spout as data source within a Flink streaming program
   2. `BoltTokenizeerWordCount` uses a bolt to split sentences into words within a Flink streaming program
      * `BoltTokenizeerWordCountWithNames` used Tuple input type and access attributes by field names (rather than index)
      * `BoltTokenizeerWordCountPOJO` used POJO input type and access attributes by field names (rather then index)

* how to submit a whole Storm topology to Flink
   3. `WordCountTopology` plugs a Storm topology together
      * `StormWordCountLocal` submits the topology to a local Flink cluster (similiar to a `LocalCluster` in Storm)
        (`StormWordCountNamedLocal` access attributes by field names rather than index)
      * `StormWordCountRemoteByClient` submits the topology to a remote Flink cluster (simliar to the usage of `NimbusClient` in Storm)
      * `StormWordCountRemoteBySubmitter` submits the topology to a remote Flink cluster (simliar to the usage of `StormSubmitter` in Storm)

Additionally, this module package the three examples word-count programs as jar files to be submitted to a Flink cluster via `bin/flink run example.jar`.
(Valid jars are `WordCount-SpoutSource.jar`, `WordCount-BoltTokenizer.jar`, and `WordCount-StormTopology.jar`)

The package `org.apache.flink.stormcompatiblitly.wordcount.stormoperators` contain original Storm spouts and bolts that can be used unmodified within Storm or Flink.
