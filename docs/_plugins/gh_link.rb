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

      "<a href=#{config["FLINK_GITHUB_URL"]}/blob/#{gh_tag}/#{path}>#{name}</a>"
    end
  end
end

Liquid::Template.register_tag('gh_link', Jekyll::GitHubLinkTag)