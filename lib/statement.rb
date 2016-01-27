module Kaftan

  class Statement

    attr_accessor :prepare_string

    def initialize(crud_name, fields, table)
      @type = crud_name
      @fields = fields
      @table = table
    end

    def build
      self.send(@type)
      self
    end

    def update
      build_update_base
      #build_meta(__callee__)
    end

    def insert
      build_insert_base
      #build_meta(__callee__)
    end

    def build_update_base
      @prepare_string = "UPDATE #{@table} SET"
      @prepare_string += variable_fields.inject("") {|str, field| str += " #{field} = ?,"; str }.chop!
      @prepare_string += " WHERE "
      @prepare_string += key_fields.inject("") {|str, field| str += " #{field} = ? AND"; str }[0..-5]
    end

    def build_insert_base
      @prepare_string = "INSERT into #{@table} ("
      @prepare_string += all_fields.inject("") {|str, field| str += "#{field},"}.chop!
      @prepare_string += ") VALUES ("
      @prepare_string += all_fields.inject("") {|str, field| str += ":#{field},"}.chop!
      @prepare_string += ")"
    end

    def key_fields
      @fields.select {|k,v| v[:key] == true}.keys
    end

    def variable_fields
      @fields.select {|k,v| !v.has_key? :key}.keys
    end

    def all_fields
      @fields.keys
    end

  end
end
