class ExportToGcloud::Exporter::Context

  attr_reader :client

  OPTIONS = %i[dump_path storage_prefix bucket dataset].freeze
  attr_reader *OPTIONS

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
    bucket = get_bucket bucket if String === bucket
    @bucket = bucket
  end

  def set_dataset dataset
    dataset = get_dataset dataset if String === dataset
    @dataset = dataset
  end

  def copy
    self.class.new client, OPTIONS.inject({}){|h, k| h[k] = send k; h}
  end

  private

  def get_bucket bucket
    client.storage.bucket bucket
  end

  def get_dataset dataset
    client.bigquery.dataset dataset
  end

end