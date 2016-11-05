require 'gcloud'
require 'gcloud/bigquery'
require 'pathname'
require 'ostruct'
require 'csv'

# large files uploading
require 'httpclient'
Faraday.default_adapter = :httpclient

# monkeypatch :/ some issue in google-api
# see http://googlecloudplatform.github.io/gcloud-ruby/docs/master/Gcloud/Storage.html
#     -> A note about large uploads
require 'google/api_client'
Faraday::Response.register_middleware gzip: Faraday::Response::Middleware

module ExportToGcloud

  def self.definitions_resolver= proc
    @definitions_resolver = proc
  end

  # waits for BigQuery jobs
  # - send a block to do something with failed
  def self.wait_for_load_jobs(jobs, &block)
    jobs_left = jobs.dup
    failed = []
    sleeper = ->(_retries) {sleep 2 * _retries + 5}
    retries = 0

    until jobs_left.empty?
      sleeper.call retries
      retries += 1
      jobs_left.each &:reload!
      jobs_left.delete_if do |j|
        if j.done?
          failed << {id: j.job_id, error: j.error, sources: j.sources} if j.failed?
          true
        end
      end
    end

    block.call failed unless failed.empty?
  end

  def self.get_exporter name, context=nil
    name = name.to_s

    @definitions ||= {}
    unless @definitions.has_key? name
      @definitions[name] = ::ExportToGcloud::Exporter::Definition.load_definition name, @definitions_resolver
    end

    definition = @definitions[name]
    definition.type.new definition, context
  end

  def self.create_context **opts
    ::ExportToGcloud::Exporter::Context.new client, opts
  end

end

require_relative 'exporters/exporter'
require_relative 'exporters/csv_exporter'
require_relative 'exporters/pg_exporter'