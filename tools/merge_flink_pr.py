#!/usr/bin/env python

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Utility for creating well-formed pull request merges and pushing them to Apache.
#   usage: ./merge_flink_pr.sh    (see config env vars below)
#
# This utility assumes you already have local a Flink git folder and that you
# have added remotes corresponding to both (i) the github apache FLINK
# mirror and (ii) the apache git repo.

#
# Note by Robert Metzger:
# This script has been written by the Apache Spark team.
# I found the source here: https://github.com/apache/spark/blob/master/dev/merge_spark_pr.py
#

import json
import os
import re
import subprocess
import sys
import tempfile
import urllib2

try:
    import jira.client
    JIRA_IMPORTED = True
except ImportError:
    JIRA_IMPORTED = False

# Location of your FLINK git development area
FLINK_HOME = os.environ.get("FLINK_HOME", "/home/patrick/Documents/spark")
# Remote name which points to the Gihub site
PR_REMOTE_NAME = os.environ.get("PR_REMOTE_NAME", "apache-github")
# Remote name which points to Apache git
PUSH_REMOTE_NAME = os.environ.get("PUSH_REMOTE_NAME", "apache")
# ASF JIRA username
JIRA_USERNAME = os.environ.get("JIRA_USERNAME", "pwendell")
# ASF JIRA password
JIRA_PASSWORD = os.environ.get("JIRA_PASSWORD", "1234")

GITHUB_BASE = "https://github.com/apache/incubator-flink/pull"
GITHUB_API_BASE = "https://api.github.com/repos/apache/incubator-flink"
JIRA_BASE = "https://issues.apache.org/jira/browse"
JIRA_API_BASE = "https://issues.apache.org/jira"
# Prefix added to temporary branches
BRANCH_PREFIX = "PR_TOOL"

os.chdir(FLINK_HOME)


def get_json(url):
    try:
        return json.load(urllib2.urlopen(url))
    except urllib2.HTTPError as e:
        print "Unable to fetch URL, exiting: %s" % url
        sys.exit(-1)


def fail(msg):
    print msg
    clean_up()
    sys.exit(-1)


def run_cmd(cmd):
    if isinstance(cmd, list):
        return subprocess.check_output(cmd)
    else:
        return subprocess.check_output(cmd.split(" "))


def continue_maybe(prompt):
    result = raw_input("\n%s (y/n): " % prompt)
    if result.lower() != "y":
        fail("Okay, exiting")


original_head = run_cmd("git rev-parse HEAD")[:8]


def clean_up():
    print "Restoring head pointer to %s" % original_head
    run_cmd("git checkout %s" % original_head)

    branches = run_cmd("git branch").replace(" ", "").split("\n")

    for branch in filter(lambda x: x.startswith(BRANCH_PREFIX), branches):
        print "Deleting local branch %s" % branch
        run_cmd("git branch -D %s" % branch)


# merge the requested PR and return the merge hash
def merge_pr(pr_num, target_ref):
    pr_branch_name = "%s_MERGE_PR_%s" % (BRANCH_PREFIX, pr_num)
    target_branch_name = "%s_MERGE_PR_%s_%s" % (BRANCH_PREFIX, pr_num, target_ref.upper())
    run_cmd("git fetch %s pull/%s/head:%s" % (PR_REMOTE_NAME, pr_num, pr_branch_name))
    run_cmd("git fetch %s %s:%s" % (PUSH_REMOTE_NAME, target_ref, target_branch_name))
    run_cmd("git checkout %s" % target_branch_name)

    had_conflicts = False
    try:
        run_cmd(['git', 'merge', pr_branch_name, '--squash'])
    except Exception as e:
        msg = "Error merging: %s\nWould you like to manually fix-up this merge?" % e
        continue_maybe(msg)
        msg = "Okay, please fix any conflicts and 'git add' conflicting files... Finished?"
        continue_maybe(msg)
        had_conflicts = True

    commit_authors = run_cmd(['git', 'log', 'HEAD..%s' % pr_branch_name,
                             '--pretty=format:%an <%ae>']).split("\n")
    distinct_authors = sorted(set(commit_authors),
                              key=lambda x: commit_authors.count(x), reverse=True)
    primary_author = distinct_authors[0]
    commits = run_cmd(['git', 'log', 'HEAD..%s' % pr_branch_name,
                      '--pretty=format:%h [%an] %s']).split("\n\n")

    merge_message_flags = []

    merge_message_flags += ["-m", title]
    if body != None:
        merge_message_flags += ["-m", body]

    authors = "\n".join(["Author: %s" % a for a in distinct_authors])

    merge_message_flags += ["-m", authors]

    if had_conflicts:
        committer_name = run_cmd("git config --get user.name").strip()
        committer_email = run_cmd("git config --get user.email").strip()
        message = "This patch had conflicts when merged, resolved by\nCommitter: %s <%s>" % (
            committer_name, committer_email)
        merge_message_flags += ["-m", message]

    # The string "Closes #%s" string is required for GitHub to correctly close the PR
    merge_message_flags += [
        "-m",
        "Closes #%s from %s and squashes the following commits:" % (pr_num, pr_repo_desc)]
    for c in commits:
        merge_message_flags += ["-m", c]

    run_cmd(['git', 'commit', '--author="%s"' % primary_author] + merge_message_flags)

    continue_maybe("Merge complete (local ref %s). Push to %s?" % (
        target_branch_name, PUSH_REMOTE_NAME))

    try:
        run_cmd('git push %s %s:%s' % (PUSH_REMOTE_NAME, target_branch_name, target_ref))
    except Exception as e:
        clean_up()
        fail("Exception while pushing: %s" % e)

    merge_hash = run_cmd("git rev-parse %s" % target_branch_name)[:8]
    clean_up()
    print("Pull request #%s merged!" % pr_num)
    print("Merge hash: %s" % merge_hash)
    return merge_hash


def cherry_pick(pr_num, merge_hash, default_branch):
    pick_ref = raw_input("Enter a branch name [%s]: " % default_branch)
    if pick_ref == "":
        pick_ref = default_branch

    pick_branch_name = "%s_PICK_PR_%s_%s" % (BRANCH_PREFIX, pr_num, pick_ref.upper())

    run_cmd("git fetch %s %s:%s" % (PUSH_REMOTE_NAME, pick_ref, pick_branch_name))
    run_cmd("git checkout %s" % pick_branch_name)
    run_cmd("git cherry-pick -sx %s" % merge_hash)

    continue_maybe("Pick complete (local ref %s). Push to %s?" % (
        pick_branch_name, PUSH_REMOTE_NAME))

    try:
        run_cmd('git push %s %s:%s' % (PUSH_REMOTE_NAME, pick_branch_name, pick_ref))
    except Exception as e:
        clean_up()
        fail("Exception while pushing: %s" % e)

    pick_hash = run_cmd("git rev-parse %s" % pick_branch_name)[:8]
    clean_up()

    print("Pull request #%s picked into %s!" % (pr_num, pick_ref))
    print("Pick hash: %s" % pick_hash)
    return pick_ref


def fix_version_from_branch(branch, versions):
    # Note: Assumes this is a sorted (newest->oldest) list of un-released versions
    if branch == "master":
        return versions[0]
    else:
        branch_ver = branch.replace("branch-", "")
        return filter(lambda x: x.name.startswith(branch_ver), versions)[-1]


def resolve_jira(title, merge_branches, comment):
    asf_jira = jira.client.JIRA({'server': JIRA_API_BASE},
                                basic_auth=(JIRA_USERNAME, JIRA_PASSWORD))

    default_jira_id = ""
    search = re.findall("FLINK-[0-9]{4,5}", title)
    if len(search) > 0:
        default_jira_id = search[0]

    jira_id = raw_input("Enter a JIRA id [%s]: " % default_jira_id)
    if jira_id == "":
        jira_id = default_jira_id

    try:
        issue = asf_jira.issue(jira_id)
    except Exception as e:
        fail("ASF JIRA could not find %s\n%s" % (jira_id, e))

    cur_status = issue.fields.status.name
    cur_summary = issue.fields.summary
    cur_assignee = issue.fields.assignee
    if cur_assignee is None:
        cur_assignee = "NOT ASSIGNED!!!"
    else:
        cur_assignee = cur_assignee.displayName

    if cur_status == "Resolved" or cur_status == "Closed":
        fail("JIRA issue %s already has status '%s'" % (jira_id, cur_status))
    print ("=== JIRA %s ===" % jira_id)
    print ("summary\t\t%s\nassignee\t%s\nstatus\t\t%s\nurl\t\t%s/%s\n" % (
        cur_summary, cur_assignee, cur_status, JIRA_BASE, jira_id))

    versions = asf_jira.project_versions("FLINK")
    versions = sorted(versions, key=lambda x: x.name, reverse=True)
    versions = filter(lambda x: x.raw['released'] is False, versions)

    default_fix_versions = map(lambda x: fix_version_from_branch(x, versions).name, merge_branches)
    for v in default_fix_versions:
        # Handles the case where we have forked a release branch but not yet made the release.
        # In this case, if the PR is committed to the master branch and the release branch, we
        # only consider the release branch to be the fix version. E.g. it is not valid to have
        # both 1.1.0 and 1.0.0 as fix versions.
        (major, minor, patch) = v.split(".")
        if patch == "0":
            previous = "%s.%s.%s" % (major, int(minor) - 1, 0)
            if previous in default_fix_versions:
                default_fix_versions = filter(lambda x: x != v, default_fix_versions)
    default_fix_versions = ",".join(default_fix_versions)

    fix_versions = raw_input("Enter comma-separated fix version(s) [%s]: " % default_fix_versions)
    if fix_versions == "":
        fix_versions = default_fix_versions
    fix_versions = fix_versions.replace(" ", "").split(",")

    def get_version_json(version_str):
        return filter(lambda v: v.name == version_str, versions)[0].raw

    jira_fix_versions = map(lambda v: get_version_json(v), fix_versions)

    resolve = filter(lambda a: a['name'] == "Resolve Issue", asf_jira.transitions(jira_id))[0]
    asf_jira.transition_issue(
        jira_id, resolve["id"], fixVersions=jira_fix_versions, comment=comment)

    print "Succesfully resolved %s with fixVersions=%s!" % (jira_id, fix_versions)


#branches = get_json("%s/branches" % GITHUB_API_BASE)
#print "branches %s " % (branches)
#branch_names = filter(lambda x: x.startswith("release-"), [x['name'] for x in branches])
# Assumes branch names can be sorted lexicographically
latest_branch = "master" #sorted(branch_names, reverse=True)[0]

pr_num = raw_input("Which pull request would you like to merge? (e.g. 34): ")
pr = get_json("%s/pulls/%s" % (GITHUB_API_BASE, pr_num))

url = pr["url"]
title = pr["title"]
body = pr["body"]
target_ref = pr["base"]["ref"]
user_login = pr["user"]["login"]
base_ref = pr["head"]["ref"]
pr_repo_desc = "%s/%s" % (user_login, base_ref)

if pr["merged"] is True:
    print "Pull request %s has already been merged, assuming you want to backport" % pr_num
    merge_commit_desc = run_cmd([
        'git', 'log', '--merges', '--first-parent',
        '--grep=pull request #%s' % pr_num, '--oneline']).split("\n")[0]
    if merge_commit_desc == "":
        fail("Couldn't find any merge commit for #%s, you may need to update HEAD." % pr_num)

    merge_hash = merge_commit_desc[:7]
    message = merge_commit_desc[8:]

    print "Found: %s" % message
    maybe_cherry_pick(pr_num, merge_hash, latest_branch)
    sys.exit(0)

if not bool(pr["mergeable"]):
    msg = "Pull request %s is not mergeable in its current form.\n" % pr_num + \
        "Continue? (experts only!)"
    continue_maybe(msg)

print ("\n=== Pull Request #%s ===" % pr_num)
print ("title\t%s\nsource\t%s\ntarget\t%s\nurl\t%s" % (
    title, pr_repo_desc, target_ref, url))
continue_maybe("Proceed with merging pull request #%s?" % pr_num)

merged_refs = [target_ref]

merge_hash = merge_pr(pr_num, target_ref)

pick_prompt = "Would you like to pick %s into another branch?" % merge_hash
while raw_input("\n%s (y/n): " % pick_prompt).lower() == "y":
    merged_refs = merged_refs + [cherry_pick(pr_num, merge_hash, latest_branch)]

if JIRA_IMPORTED:
    continue_maybe("Would you like to update an associated JIRA?")
    jira_comment = "Issue resolved by pull request %s\n[%s/%s]" % (pr_num, GITHUB_BASE, pr_num)
    resolve_jira(title, merged_refs, jira_comment)
else:
    print "Could not find jira-python library. Run 'sudo pip install jira-python' to install."
    print "Exiting without trying to close the associated JIRA."
