module Jekyll
  module Tocify
    def tocify(input)
      converter = Redcarpet::Markdown.new(Redcarpet::Render::HTML_TOC.new(nesting_level: 2))
      converter.render(input)
    end
  end
end

Liquid::Template.register_filter(Jekyll::Tocify)
