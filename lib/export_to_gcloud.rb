require_relative 'export_to_gcloud/version'

module ExportToGcloud

  def self.setup project_name:, config_file:, definitions_resolver:nil
    require_relative 'export_to_gcloud/library'

    self.definitions_resolver = definitions_resolver if definitions_resolver
    @client = ::Gcloud.new project_name, config_file
  end

  def self.client
    @client || raise('Gcloud client not present. call ExportToGcloud#setup first.')
  end

end