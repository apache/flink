module Jekyll
  module Tocify
    def tocify(input)
      converter = Redcarpet::Markdown.new(Redcarpet::Render::HTML_TOC)
      converter.render(input)
    end
  end
end

Liquid::Template.register_filter(Jekyll::Tocify)
