class ExportToGcloud::Exporter::Options

  attr_reader :project

  private OPTIONS = %i[dump_path storage_prefix bucket dataset]

  def initialize project, **opts
    @project = project
    set opts
  end

  def set **opts
    OPTIONS.each do |key|
      value = opts[key]
      send "set_#{key}", value if value
    end
  end

  def set_dump_path path
    @dump_path = Pathname.new path
  end

  def set_storage_prefix prefix
    @storage_prefix = prefix
  end

  def set_bucket bucket
    bucket = project.storage.bucket bucket if String === bucket
    @bucket = bucket
  end

  def set_dataset dataset
    dataset = project.bigquery.dataset dataset if String === dataset
    @bucket = dataset
  end

  OPTIONS.each do |key|
    define_method key do
      value = instance_variable_get "@#{key}"
      value || "Undefined value for #{key} in exporter options!"
    end
  end

end