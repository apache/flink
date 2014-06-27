---
title:  "Contribute"
---

The Flink project welcomes all sorts contributions in the form of code (improvements, features, bugfixes), documentation, tests, and community participation (discussions & questions).



## Easy Issues for Starters

We maintain all known issues and feature drafts in the [Flink project JIRA](https://issues.apache.org/jira/browse/FLINK-989?jql=project%20%3D%20FLINK).

We also try to maintain a [list of simple "starter issues"](https://issues.apache.org/jira/browse/FLINK-933?jql=project%20%3D%20FLINK%20AND%20labels%20%3D%20starter) that we believe are good tasks for new contributors. Those tasks are meant to allow people to get into the project and become familiar with the process of contributing. Feel free to ask questions about issues that you would be interested in working on.

In addition, you can find a list of ideas for projects and improvements in the [projects wiki page](http://the/wiki/url).



## Contributing Code & Documentation

This section gives you a brief introduction in how to contribute code and documentation to Flink. We maintain both the code and the documentation in the same repository, so the process is essentially the same for both. We use [git](http://git-scm.com/) for the code and documentation version control.

The Flink project accepts code contributions though the [GitHub Mirror](https://github.com/apache/incubator-flink), in the form of [Pull Requests](https://help.github.com/articles/using-pull-requests). Pull requests are basically a simpler way of offering a patch, by providing a pointer to a code branch that contains the change.


### Setting up the Infrastructure and Creating a Pull Request

1. The first step is to create yourself a copy of the Flink code base. We suggest to fork the [Flink GitHub Mirror Repository](https://github.com/apache/incubator-flink) into your own [GitHub](https://github.com) account. You need to register on GitHub for that, if you have no account so far.

2. Next, clone your repository fork to your local machine.
```
git clone https://github.com/<your-user-name>/incubator-flink.git
```

3. It is typically helpful to switch to a *topic branch* for the changes. To create a dedicated branch based on the current master, use the following command:
```
git checkout -b myBranch master
```

4. Now you can create your changes, compile the code, and validate the changes. Here are some pointers on how to [set up the Eclipse IDE for development](https://github.com/apache/incubator-flink/#eclipse-setup-and-debugging), and how to [build the code](https://github.com/apache/incubator-flink/#build-stratosphere).

5. After you have finalized your contribution, verify the compliance with the contribution guidelines (see below), and commit them. To make the changes easily mergeable, please rebase them to the latest version of the main repositories master branch. Assuming you created a topic branch (step 3), you can follow this sequence of commands to do that:
Switch to the master branch, update it to the latest revision, switch back to your topic branch, and rebase it on top of the master branch.
```
git checkout master
git pull https://github.com/apache/incubator-flink.git master
git checkout myBranch
git rebase master
```
Have a look [here](https://help.github.com/articles/using-git-rebase) for more information about rebasing commits.


6. Push the contribution it back into your fork of the Flink repository.
```
git push origin myBranch
```
Go the website of your repository fork (`https://github.com/<your-user-name>/incubator-flink`) and use the "Create Pull Request" button to start creating a pull request. Make sure that the base fork is `apache/incubator-flink master` and the head fork selects the branch with your changes. Give the pull request a meaningful description and send it.


### Verifying the Compliance of your Code

Before sending a patch or pull request, please verify that it complies with the guidelines of the project. While we try to keep the set of guidelines small and easy, it is important to follow those rules in order to guarantee good code quality, to allow efficient reviews, and to allow committers to easily merge your changes.

Please have a look at the [coding guidelines](coding_guidelines.html) for a guide to the format of code and pull requests.

Most important of all, verify that your changes are correct and do not break existing functionality. Run the existing tests by calling `mvn verify` in the root directory of the repository, and make sure that the tests succeed. We encourage every contributor to use a *continuous integration* service that will automatically test the code in your repository whenever you push a change. Flink is pre-configured for [Travis CI](http://docs.travis-ci.com/), which can be easily enabled for your private repository fork (it uses GitHub for authentication, so you so not need an additional account). Simply add the *Travis CI* hook to your repository (*settings --> webhooks & services --> add service*) and enable tests for the "incubator-flink" repository on [Travis](https://travis-ci.org/profile).

When contributing documentation, please review the rendered HTML versions of the documents you changed. You can look at the HTML pages by using the rendering script in preview mode. 
```
cd docs
./build_docs.sh -p
```
Now, open your browser at `http://localhost:4000` and check out the pages you changed.



## How to become a committer

There is no strict protocol for becoming a committer. Candidates for new committers are typically people that are active contributors and community members.

Being an active community member means participating on mailing list discussions, helping to answer questions, being respectful towards others, and following the meritocratic principles of community management. Since the "Apache Way" has a strong focus on the project community, this part is very important.

Of course, contributing code to the project is important as well. A good way to start is contributing improvements, new features, or bugfixes. You need to show that you take responsibility for the code that you contribute, add tests/documentation, and help maintaining it. 

Finally, candidates for new committers are suggested by current committers, mentors, or PMC members, and voted upon by the PMC.

