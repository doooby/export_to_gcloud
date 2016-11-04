require_relative 'lib/export_to_gcloud/version'

Gem::Specification.new do |s|
  s.name        = 'export_to_gcloud'
  s.version     = ExportToGcloud::VERSION

  s.date        = Date.today.to_s
  s.summary     = 'Exporter to BigQuery'
  s.description = 'A simple helper to export data to BigQuery via Google Drive'

  s.authors     = ['Ondřej Želazko']
  s.email       = 'zelazk.o@email.cz'

  s.files       = Dir[ %w[LICENSE lib/**/*.rb] ]

  # s.license       = 'MIT'

  s.add_dependency 'gcloud'
  s.add_dependency 'httpclient'
end