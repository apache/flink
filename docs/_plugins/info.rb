module Jekyll
  class InfoTag < Liquid::Tag

    def initialize(tag_name, text, tokens)
      super
      @text = text
    end

    def render(context)
    	if @text.to_s == ''
    		@text = "Info"
    	end

    	@text = @text.strip! || @text if !@text.nil?
    	"<span class=\"label label-info\">#{@text}</span>"
    end
  end
end

Liquid::Template.register_tag('info', Jekyll::InfoTag)
