<!--
*Thank you very much for contributing to Apache Flink - we are happy that you want to help us improve Flink. To help the community review your contribution in the best possible way, please go through the checklist below, which will get the contribution into a shape in which it can be best reviewed.*

*Please understand that we do not do this to make contributions to Flink a hassle. In order to uphold a high standard of quality for code contributions, while at the same time managing a large number of contributions, we need contributors to prepare the contributions well, and give reviewers enough contextual information for the review. Please also understand that contributions that do not follow this guide will take longer to review and thus typically be picked up with lower priority by the community.*

## Contribution Checklist

  - Make sure that the pull request corresponds to a [JIRA issue](https://issues.apache.org/jira/projects/FLINK/issues). Exceptions are made for typos in JavaDoc or documentation files, which need no JIRA issue.
  
  - Name the pull request in the form "[FLINK-XXXX] [component] Title of the pull request", where *FLINK-XXXX* should be replaced by the actual issue number. Skip *component* if you are unsure about which is the best component.
  Typo fixes that have no associated JIRA issue should be named following this pattern: `[hotfix] [docs] Fix typo in event time introduction` or `[hotfix] [javadocs] Expand JavaDoc for PuncuatedWatermarkGenerator`.

  - Fill out the template below to describe the changes contributed by the pull request. That will give reviewers the context they need to do the review.
  
  - Make sure that the change passes the automated tests, i.e., `mvn clean verify` passes. You can set up Azure Pipelines CI to do that following [this guide](https://cwiki.apache.org/confluence/display/FLINK/Azure+Pipelines#AzurePipelines-Tutorial:SettingupAzurePipelinesforaforkoftheFlinkrepository).

  - Each pull request should address only one issue, not mix up code from multiple issues.
  
  - Each commit in the pull request has a meaningful commit message (including the JIRA id)

  - Once all items of the checklist are addressed, remove the above text and this checklist, leaving only the filled out template below.


**(The sections below can be removed for hotfixes of typos)**
-->

## What is the purpose of the change

*(For example: This pull request makes task deployment go through the blob server, rather than through RPC. That way we avoid re-transferring them on each deployment (during recovery).)*


## Brief change log

*(for example:)*
  - *The TaskInfo is stored in the blob store on job creation time as a persistent artifact*
  - *Deployments RPC transmits only the blob storage reference*
  - *TaskManagers retrieve the TaskInfo from the blob cache*


## Verifying this change

*(Please pick either of the following options)*

This change is a trivial rework / code cleanup without any test coverage.

*(or)*

This change is already covered by existing tests, such as *(please describe tests)*.

*(or)*

This change added tests and can be verified as follows:

*(example:)*
  - *Added integration tests for end-to-end deployment with large payloads (100MB)*
  - *Extended integration test for recovery after master (JobManager) failure*
  - *Added test that validates that TaskInfo is transferred only once across recoveries*
  - *Manually verified the change by running a 4 node cluser with 2 JobManagers and 4 TaskManagers, a stateful streaming program, and killing one JobManager and two TaskManagers during the execution, verifying that recovery happens correctly.*

## Does this pull request potentially affect one of the following parts:

  - Dependencies (does it add or upgrade a dependency): (yes / no)
  - The public API, i.e., is any changed class annotated with `@Public(Evolving)`: (yes / no)
  - The serializers: (yes / no / don't know)
  - The runtime per-record code paths (performance sensitive): (yes / no / don't know)
  - Anything that affects deployment or recovery: JobManager (and its components), Checkpointing, Kubernetes/Yarn, ZooKeeper: (yes / no / don't know)
  - The S3 file system connector: (yes / no / don't know)

## Documentation

  - Does this pull request introduce a new feature? (yes / no)
  - If yes, how is the feature documented? (not applicable / docs / JavaDocs / not documented)
