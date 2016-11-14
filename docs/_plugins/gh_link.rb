# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
# 
# http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# ---------------------------------------------------------
# Expands a github link shortcut into a proper markdown link
# ---------------------------------------------------------

module Jekyll
  class GitHubLinkTag < Liquid::Tag

    def initialize(tag_name, input, tokens)
      super
      @input = input
    end

    def render(context)
      input = @input.sub(/".*"/, "").split
      name = @input.match(/".*"/).to_s.gsub(/"/, "")#@input.split.drop(2).join(" ")
      config = context.registers[:site].config

      path = input[0]
      file = path.split('/').last

      page_gh_tag = context["page"]["gh_link_tag"]
      # tag precendence:
      # 1. input[1],
      # 2. 'gh_link_tag' of page frontmatter
      # 3. "master" (default)
      gh_tag = input[1].nil? ? (page_gh_tag.nil? ? "master" : page_gh_tag) : input[1]
      name = name.to_s == '' ? file : name
      #refname = input[2].nil? ? file : input[2]

      "[#{name}](#{config["github_url"]}/blob/#{gh_tag}/#{path})"
    end
  end
end

Liquid::Template.register_tag('gh_link', Jekyll::GitHubLinkTag)
