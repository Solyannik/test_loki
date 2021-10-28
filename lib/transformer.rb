class Transformer
  def initialize(value)
    @name = value
    @parts = []
  end

  def perform
    change_words
    delete_point
    split_by_slash
    add_parentheses
    rotate_parts
    normalize_string
    @name
  end

private

  def change_words
    words = {
      "Twp" => "Township",
      "Hwy" => "Highway",
      "CCH" => "Country Club Hills"
    }
    words.each { |k,v| @name.gsub!(k,v)}
  end

  def delete_point
    @name.delete!('.')
  end

  def split_by_slash
    @parts = @name.split('/').map(&:strip)
  end

  def add_parentheses
    @parts = @parts.map do |part|
      if part.include? ?,
        comma_parts = part.split(',').map(&:strip)
        comma_parts.first.downcase!
        comma_parts[comma_parts.size-1] = comma_parts.last.prepend('(') << ')'
        part = comma_parts.join(' ')
      end
      part
    end
  end

  def rotate_parts
    for _ in 1..@parts.size - 1 do
      @parts = @parts.rotate(1)
    end
  end

  def normalize_string
    @parts.map { |i| i.downcase! } if @parts.size == 1
    @parts.drop(1).map { |i| i.downcase! } if @parts.size > 1 && !@parts.join(' ').match(/[()]/)
    @parts.insert(-2, 'and') if @parts.size > 2
    @name = @parts.join(' ')
    max = @name.scan(/\S+/).size
    1.upto(max).each_with_object(@name) do |i, n|
      n.gsub!(/((?:\b\s*[A-z]+){#{i}})\1/i, '\1')
    end
  end
end
