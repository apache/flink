module Jekyll
  class TopTag < Liquid::Tag

    def initialize(tag_name, text, tokens)
      super
    end

    def render(context)
    	"<a href=\"\#top\" class=\"top pull-right\"><span class=\"glyphicon glyphicon-chevron-up\"></span> Back to top</a>"
    end
  end
end

Liquid::Template.register_tag('top', Jekyll::TopTag)
