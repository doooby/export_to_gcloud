class ExportToGcloud::Exporter::Context

  attr_reader :client

  OPTIONS = %i[dump_path storage_prefix bucket dataset].freeze

  def initialize client, **opts
    @client = client
    set opts
  end

  def set **opts
    OPTIONS.each do |key|
      value = opts[key]
      send "set_#{key}", value if value
    end
    self
  end

  def set_dump_path path
    @dump_path = Pathname.new path
  end

  def set_storage_prefix prefix
    @storage_prefix = prefix
  end

  def set_bucket bucket
    bucket = client.storage.bucket bucket if String === bucket
    @bucket = bucket
  end

  def set_dataset dataset
    dataset = client.bigquery.dataset dataset if String === dataset
    @dataset = dataset
  end

  OPTIONS.each do |key|
    define_method key do
      value = instance_variable_get "@#{key}"
      value || raise("Undefined value for #{key} in exporter options!")
    end
  end

  def copy
    self.class.new client, OPTIONS.inject({}){|k, h| h[k] = instance_variable_get "@#{k}"; h}
  end

end