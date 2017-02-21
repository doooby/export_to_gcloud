### export_to_gcloud
A custom bundle to simplify upload to google's BigQuery.
<br >
Uses google storage to hold exported csv data (compressed with pigz).

definition :
```ruby
ExportToGcloud::CSVExporter.define name: 'books' do |definition|
    definition.bq_schema = Proc.new do |s|
        s.string 'title'
        s.integer 'author_id'
        s.timestamp 'release_date'
        s.integer 'page_length'
    end
    
    definition.data = Proc.new do |part|
    [
      ['What is the name of this book?', 1, Date.new(2011,1,1).to_i, 100],
      ['Not another name', 2, Date.new(2011,1,1).to_i, 100],
      ['NaN', 3, Date.new(2011,1,1).to_i, 100],
    ][part]
    end
  
    definition.parts = Proc.new do |exporter|
      exporter.add_data_part 1, label: 'part1'
      exporter.add_data_part 2, label: 'part2'
      exporter.add_data_part 3, label: 'part3'
    end
end
```

rake task or something:
```ruby
ExportToGcloud.setup project_name: 'cool-project-101',
    config_file: config_path,
    definitions_resolver: ->(name){
      Pathname.new('/path/to/definitions').join "#{name}.rb"
    }
context = ExportToGcloud.create_context dump_path: Pathname.new('/path/to/outputs'),
    bucket: 'google-storage-bucket-name', storage_prefix: 'exports/',
    dataset: 'google-big-query-dataset-name'
    
export = ExportToGcloud.get_exporter 'books', context
jobs = export.process_all_parts!
ExportToGcloud.wait_for_load_jobs jobs do |fail_infos|
  error = %Q(jobs failed:\n#{fail_infos.map(&:to_s).join "\n"})
  raise error
end
```

### status
- in production for exporting on daily basis
- missing docs
- missing any integration tests; few unit tests are pending

### dev
- `bundle install`
- `autotest` (or just `bundle exec rspec`)