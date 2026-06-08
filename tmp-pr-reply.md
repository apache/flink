Thanks @davidradl, appreciate you sharing these.

**Reusable polling utility** — the two polling spots already go through shared helpers: `SplitFetcherManagerTest` uses `org.apache.flink.test.util.TestUtils.waitUntil`, and `RescaleTimelineITCase.waitUntilConditionWithTimeout` is a pre-existing private wrapper around `CommonTestUtils.waitUntilCondition` (this PR only adds a call to it). So there's no new duplication introduced here. Promoting that wrapper into a shared util touches a broadly-used test utility and is really a separate refactor — I'd keep it out of this `[hotfix]` to honor the "separate cleanup from functional changes" convention, and can file a follow-up JIRA if it's wanted.

**Document timeout values** — good call. The 30s wait in `SplitFetcherManagerTest` already got an explanatory comment; I've now added a brief note on the new 10s timeout in `RescaleTimelineITCase` explaining why it's a generous upper bound rather than a load-bearing value.

**`!t.isAlive()` check** — thanks, glad that read well.