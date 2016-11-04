# require 'gcloud/bigquery'
#
# module ExportToGcloud
#
#   class Exporter
#
#     STORAGE_KEY_PREFIX = 'yeti_export'
#
#     attr_reader :local_name
#     attr_writer :bq_schema_builder, :sql_query, :table_data
#
#     def initialize(local_name, quote_string_field=true)
#       @local_name = local_name
#       @quote_string_field = quote_string_field
#     end
#
#     def file_names(label)
#       file = "#{local_name}_#{label}.csv"
#       return "#{STORAGE_KEY_PREFIX}/#{file}", file
#     end
#
#     def bq_schema_builder
#       @bq_schema_builder || raise("Undefined BQ table bq_schema_builder!")
#     end
#
#     ### partition for bigger tables
#
#     def add_source_part(*part_data, label:)
#       @parts ||= []
#       part_data.unshift label.to_s || (@parts.length+1).to_s
#       @parts << part_data
#     end
#
#     def upload_data_start_importing
#       @parts ||= [['all']]
#       @parts.map do |label, *part_data|
#         storage_name, name = file_names label
#         file_path = Yeti::Gcloud.tmp_directory.join name
#
#         build_source_part! file_path, *part_data
#         upload_file! file_path, storage_name
#         start_load_job gcloud_file
#       end
#     end
#
#     def get_storage_files
#       @parts.map do |label, *_|
#         storage_name, _ = file_names label
#         Yeti::Gcloud.storage_bucket.file storage_name
#       end.compact
#     end
#
#     ### exporting part of process
#
#     def build_source_part! file_path, *part_data
#       if @sql_query
#         copy_from_pg_to_file! file_path.to_path, @sql_query, *part_data
#
#       elsif @table_data
#         put_data_to_file! file_path.to_path, @table_data, *part_data
#
#       end
#     end
#
#     def upload_file!(file_path, storage_name)
#       file_path = compress_source_file! file_path
#       gcloud_file = Yeti::Gcloud.storage_bucket.create_file file_path, storage_name, chunk_size: 2*1024*1024
#       file_path.delete
#       gcloud_file
#     end
#
#     def copy_from_pg_to_file! file, sql, *part_data
#       sql = case sql
#         when Proc then sql.call *part_data
#         when String then sql
#         else raise "SQL query to fetch the data is not defined (table: #{local_name})!"
#       end
#
#       schema = ::Gcloud::Bigquery::Table::Schema.new nil
#       bq_schema_builder.call schema
#       string_fileds_names = schema.fields.select{|f| f['type']=='STRING'}.map{|f| f['name']}
#
#       force_quote = if !@quote_string_field || string_fileds_names.empty?
#         ''
#       else
#         ", FORCE_QUOTE (#{string_fileds_names.join ', '})"
#       end
#       sql = "COPY (#{sql}) TO '#{file}' WITH (FORMAT CSV, DELIMITER ';', QUOTE '\"'#{force_quote});"
#
#       ActiveRecord::Base.connection.execute sql
#     end
#
#     def put_data_to_file! file, data, *part_data
#       data = data.call *part_data if Proc === data
#
#       csv_data = CSV.generate col_sep: ';', force_quotes: true do |csv|
#         data.each{|row| csv << row}
#       end
#
#       File.write file, csv_data
#     end
#
#     def compress_source_file!(original_file)
#       err = %x(pigz -f9 #{original_file.to_path} 2>&1)
#       compressed_file = Pathname.new "#{original_file.to_path}.gz"
#       raise "Compression failed: #{err}" unless compressed_file.exist?
#       original_file.delete if original_file.exist?
#       compressed_file
#     end
#
#     ### import to BQ part
#
#     def connect_bq_table(dataset, table_name)
#       @bq_dataset = dataset
#       @bq_table_name = table_name
#       @bq_table = nil
#     end
#
#     def bq_table
#       raise 'conect to BQ table first (using #connect_bq_table)' unless @bq_dataset && @bq_table_name
#       @bq_table ||= @bq_dataset.table(@bq_table_name)
#     end
#
#     def recreate_table!
#       bq_table.try &:delete
#       @bq_table = @bq_dataset.create_table @bq_table_name, &bq_schema_builder
#     end
#
#     def start_load_job(gcloud_file, **_load_settings)
#       load_settings = {
#           format: 'csv',
#           quote: '"',
#           delimiter: ';',
#           create: 'never',
#           write: 'append',
#           max_bad_records: 0
#       }
#       load_settings.merge! _load_settings unless _load_settings.empty?
#       bq_table.load gcloud_file, **load_settings
#     end
#
#     ############### CLASS methods ###############
#
#     class << self
#
#       def load_source(table_name)
#         file = Rails.root.join(*%W(db gcloud_export_definitions #{table_name}.rb))
#         load file
#         sources_definitions[table_name] || raise("File #{file} must define source table #{table_name}!")
#       end
#
#       def define(table_name)
#         source = new table_name
#         yield source
#         sources_definitions[source.local_name.to_s] = source
#       end
#
#       def sources_definitions
#         @definitions ||= {}
#       end
#
#     end
#
#   end
#
# end