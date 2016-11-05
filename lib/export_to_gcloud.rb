require_relative 'export_to_gcloud/version'

module ExportToGcloud

  attr_reader :client

  def self.setup project_name, config_file, definitions_resolver:nil
    require_relative 'export_to_gcloud/library'

    self.definitions_resolver = definitions_resolver if definitions_resolver
    @client = ::Gcloud.new project_name, config_file
  end

end