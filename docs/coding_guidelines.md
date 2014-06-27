---
title:  "Coding Guidelines"
---

These are the coding and style guidelines that we follow in the Flink project.

## Guidelines for pull requests and patches

- A pull request should relate to a JIRA issue; create an issue if none exists for the change you want to make. The latest commit message should reference that issue. An example commit message would be "[FLINK-633] Fix NullPointerException for empty UDF parameters". That way, the pull request automatically gives a description of what it does, for example what bug does it fix in what way?

- We consider pull requests as requests to merge the referenced code *as is* into the current *stable* master branch. Therefore, a pull request should not be "work in progress". Open a pull request if you are confident that it can be merged into the current master branch without problems. If you rather want comments on your code, post a link to your working branch.

- Please do not combine various unrelated changes in a single pull request. Rather, open multiple individual pull requests. This ensures that pull requests are *topic related*, can be merged more easily, and typically result in topic-specific merge conflicts only.

- Any pull request where the tests do not pass or which does not compile will not undergo any further review. We recommend to connect your private GitHub accounts with [Travis CI](http://travis-ci.org/) (like the Flink GitHub repository). Travis will run tests for all tested environments whenever you push something into *your* Github repository.

- Please keep reformatting of source files to a minimum. Diffs become unreadable if you (or your IDE automatically) remove or replace whitespaces, reformat code, or comments. Also, other patches that affect the same files become un-mergeable. Please configure your IDE such that code is not automatically reformatted. Pull requests with excessive or unnecessary code reformatting might be rejected.

- All new features need to be backed by tests, *strictly*. It is very easy that a later merge accidentally throws out a feature or breaks it. This will not be caught if the feature is not guarded by tests. Anything not covered by a test is considered cosmetic.

- Before opening a pull request follow this checklist:
 - Rebase onto the latest version of the master branch
 - Clean up your commits, i.e., squash them in a reasonable way and give meaningful commit messages
 - Run *all* tests either locally with ```mvn clean verify``` or use Travis CI to check the build

- When you get comments on the pull request asking for changes, append commits for these changes. *Do not rebase and squash them.* It allows people to review the cleanup work independently. Otherwise reviewers have to go through the entire set of diffs again.

- Public methods and classes that are part of the user-facing API need to have JavaDocs. Please write meaningful docs. Good docs are concise and informative.

- Give meaningful exception messages. Try to imagine why an exception could be thrown (what a user did wrong) and give a message that will help a user to resolve the problem.

- Follow the checkstyle rules (see below). The checkstyle plugin verifies these rules when you build the code. If your code does not follow the checkstyle rules, Maven will not compile it and consequently the build will fail.


## Coding Style Guidelines

- Make sure you have Apache License headers in your files. The RAT plugin is checking for that when you build the code.

- We are using tabs for indentation, not spaces. We are not religious there, it just happened to be the way that we started with tabs, and it is important to not mix them (merge/diff conflicts).

- All statements after `if`, `for`, `while`, `do`, ... must always be encapsulated in a block with curly braces (even if the block contains one statement):
```
for (...) {
 ...
}
```
If you are wondering why, recall the famous [*goto bug*](https://www.imperialviolet.org/2014/02/22/applebug.html) in Apple's SSL library.


-  Do not use wildcard imports in the core files. They can cause problems when adding to the code and in some cases even during refactoring. Exceptions are the Tuple classes, Tuple-related utilities, and Flink user programs, when importing operators/functions. Tests are a special case of the user programs.
  
- Remove all unused imports.

- Do not use raw generic types, unless strictly necessary (sometime necessary for signature matches, arrays).

- Add annotations to suppress warnings, if they cannot be avoided (such as "unchecked", or "serial")

- Do not add "non-javadoc" comments. 

- Add comments to your code. What is it doing? Add JavaDocs or inherit them by not adding any comments to the methods. Do not automatically generate comments and avoid unnecessary comments like
```
i++;  // increment by one
```


