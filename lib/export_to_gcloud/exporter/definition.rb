class ExportToGcloud::Exporter::Definition < OpenStruct

  def initialize exporter_type, attrs
    super attrs.merge!(type: exporter_type)
  end

  def create_exporter project
    type.new self, project
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

  def self.set_last_definition klass, attrs
    last_definition = new klass, attrs
    yield last_definition if block_given?

    last_definition.validate!
    @last_definition = last_definition
  end

  def self.get_last_definition
    definition = @last_definition
    @last_definition = nil
    definition
  end

end