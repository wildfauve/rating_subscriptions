module Kaftan

  module ClassMethods
    def prepare(name, statement)
      @statements ||= {}
      @statements[name] = {}
      if statement.class == Kaftan::Statement
        prepare_string = statement.prepare_string
        @statements[name][:statement] = statement
      else
        prepare_string = statement
      end
      @statements[name][:prepare] = $session.prepare(prepare_string)
    end

    def table(name)
      @table = name
    end

    def statement(name)
      state = @statements[name]
      state.has_key?(:statement) ? state : state[:prepare]
    end

    def field(field_name, type)
      @fields ||= {}
      @fields[field_name] = type
      define_method(field_name) do
        instance_variable_get("@#{field_name}")
      end
      define_method("#{field_name}=") do |val|
        convert_val = convert_set_val_to_type(type, val)
#        if type[:type] == :set
#          inst_val = instance_variable_get("@#{field_name}")
#          binding.pry
#          inst_val.class == Set ? set_val = inst_val.merge(convert_val) : set_val = convert_val
#        else
        set_val = convert_val
#        end
        instance_variable_set("@#{field_name}", set_val)
        convert_val
      end
    end

    def fields
      @fields
    end

    def build_default_prepares
      self.prepare(:update, Kaftan::Statement.new(:update, self.fields, @table).build)
      self.prepare(:insert, Kaftan::Statement.new(:insert, self.fields, @table).build)
    end

  end

  def self.included(base)
    base.extend(ClassMethods)
  end


  def init(db_data)
    self.class.fields.keys.each {|field| self.send("#{field}=", db_data[field.to_s]) if db_data[field.to_s]}
    @db_init = true
    self
  end

  def insert
    puts "===> create"
    statement = self.class.statement(__callee__)[:statement]
    $session.execute(statement.prepare_string, arguments: model_to_arguments_hash(statement))
  end

  def update
    puts "===> update"
    statement = self.class.statement(__callee__)[:statement]
    $session.execute(statement.prepare_string, arguments: model_to_arguments_array(statement))
  end

  def new?
    @db_init ? false : true
  end

  def model_to_arguments_hash(statement)
    statement.all_fields.inject({}) {|args, field| args[field] = self.send(field); args}
  end

  def model_to_arguments_array(statement)
    statement.variable_fields.inject([]) {|args, field| args << self.send(field); args}
                              .concat(statement.key_fields.inject([]) {|args, field| args << self.send(field); args})
  end


  def convert_set_val_to_type(type, val)
    puts "type: #{type}, val: #{val}"
    if type[:with]
      self.send(type[:with], val)
    else
      coerse_type(type, val)
    end
  end

  def coerse_type(type, val)
    case type[:type]
    when :text
      if val == String
        val
      else
        val.to_s
      end
    when :decimal
      if val.class == BigDecimal
        val
      elsif val.class == Integer
        BigDecimal.new(val, 6)
      else
        binding.pry
      end
    when :time
      if val.class == Time
        val
      else
        Time.parse(val)
      end
    when :set
      if val.class == Set
        val
      else
        Set.new([val])
      end
    else
      raise ArgumentError
    end
  end

  def day_from_time(time)
    Time.new(time.year, time.month, time.day, 0,0,0,"+00:00")
  end


end
