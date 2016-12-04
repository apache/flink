# TODO

# QUESTIONS

* [fault tolerance](dev/batch/fault_tolerance) mixes batch and streaming in a confusing way
* the [info about mongodb](dev/batch/connectors) seems to be stale. There's an indirect pointer to https://flink.incubator.apache.org/news/2014/01/28/querying_mongodb.html which doesn't exist
* [batch#dataset-transformations](http://localhost:4000/dev/batch/#dataset-transformations) has strong overlap with [batch/dataset_transformations](http://localhost:4000/dev/batch/dataset_transformations.html). Don't like the duplication, but I'm not sure what to do about it.
* [local execution](dev/local_execution) and [cluster execution](dev/cluster_execution) have already been moved under batch, because their current content is batch specific. It's not clear these pages should still exist. Some of their content has already been generalized elsewhere ([linking with flink](dev/linking_with_flink)).

# NOT NOW

* example with connected streams (from training slides)
* document StreamingExecutionContext#readFile with the connectors
* checkpointing doc is incomplete
* the explanation of windowAll needs love
* find a way to raise the visibility of rich functions
* break up the streaming overview somewhat to better group material that goes with content on other pages, and to raise the discoverability of certain topics (since the sidebar navigation can only link to whole pages)
* also break up and reorganize [Basic API Concepts](dev/api_concepts) somewhat
* gather together (some of) the material on debugging
* rewrite the wikipedia example in scala
* make scala the default for all code examples

# GOALS

* make a good first impression on first-time visitors
* reduce duplication: ideally every piece of information would have one natural place to be
* improve navigation and discoverability: important topics shouldn't be hard to find
* update/remove out-of-date material
* arrange all of the content in a natural, linear ordering for those who want to read (or skim through) everything
* improve important sections that are difficult to understand
