class ExportToGcloud::Exporter::Definition < OpenStruct

  def initialize exporter_type, attrs
    super attrs.merge!(type: exporter_type)
  end

  def create_exporter
    type.new self
  end

  def validate!
    (String === name && !name.empty?)   || raise('`name` must be defined!')
    Proc === bq_schema                  || raise('`bq_schema` must be defined as a Proc!')
    data                                || raise('`data` must be defined!')
    type.validate_definition! self if type.respond_to? 'validate_definition!'
  end

  def get_data *args
    Proc === data ? data[*args] : data
  end

  def get_bq_table_name
    bq_table_name || name
  end

  def self.set_last_definition klass, attrs={}, &block
    last_definition = new klass, attrs
    block[last_definition] if block

    last_definition.validate!
    @last_definition = last_definition
  end

  def self.load_definition name, finder
    file_path = finder[name]
    load file_path
    definition = @last_definition
    @last_definition = nil

    unless definition
      raise("File #{file_path.to_s} must define exporter for '#{name}'!")
    end

    unless definition.name == name
      raise "File #{file_path.to_s} defines '#{definition.name}' instead of '#{name}'"
    end

    definition
  end

end