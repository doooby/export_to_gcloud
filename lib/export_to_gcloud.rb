require_relative 'export_to_gcloud/version'
require_relative 'export_to_gcloud/project'

module ExportToGcloud

  # since the whole export is meant to be execute a some rake task (i.e. outside main app process)
  # rather require dependencies on access then on app load
  def self.require_dependencies
    return if @deps_loaded

    require 'gcloud'

    # large files uploading
    require 'httpclient'
    Faraday.default_adapter = :httpclient

    # monkeypatch :/ some issue in google-api
    # see http://googlecloudplatform.github.io/gcloud-ruby/docs/master/Gcloud/Storage.html
    #     -> A note about large uploads
    require 'google/api_client'
    Faraday::Response.register_middleware gzip: Faraday::Response::Middleware

    require_relative 'export_to_gcloud/exporter'

    @deps_loaded = true
  end

end