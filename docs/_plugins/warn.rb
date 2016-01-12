module Jekyll
  class WarnTag < Liquid::Tag

    def initialize(tag_name, text, tokens)
      super
      @text = text
    end

    def render(context)
    	if @text.to_s == ''
    		@text = "Warning"
    	end

    	@text = @text.strip! || @text if !@text.nil?
    	"<span class=\"label label-danger\">#{@text}</span>"
    end
  end
end

Liquid::Template.register_tag('warn', Jekyll::WarnTag)
