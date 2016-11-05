
module ExportToGcloud

  class Exporter

    attr_accessor :context

    def initialize definition
      @definition = definition
      @parts = []
      case definition.parts
        when Array then definition.parts.each{|label, *part_args| add_data_part *part_args, label: label}
        when Proc  then definition.parts.call self
      end
    end

    def with_context context
      former_context = self.context
      self.context = context
      yield
      self.context = former_context
    end

    def local_file_path label
      context.dump_path.join "#{@definition.name}_#{label}.csv"
    end

    def storage_file_path label
      prefix = @definition.storage_prefix || context.storage_prefix
      "#{prefix}#{@definition.name}_#{label}.csv"
    end

    def add_data_part *args, label:nil
      args.unshift(label ? label.to_s : (@parts.length+1).to_s)
      @parts << args
    end

    def process_all_parts! recreate_table=true
      add_data_part label: 'all' if @parts.empty?

      recreate_bq_table! if recreate_table

      @parts.map do |label, *part_args|
        file = local_file_path label
        storage_name = storage_file_path label

        create_data_file! file, *part_args
        gcloud_file = upload_file! file, storage_name
        start_load_job gcloud_file
      end
    end

    def create_data_file! file, *part_data
      File.write file.to_path, @definition.get_data(*part_data)
    end

    def upload_file!(file, storage_name)
      file = compress_file! file
      gcloud_file = context.bucket.create_file file, storage_name, chunk_size: 2**21 # 2MB
      file.delete
      gcloud_file
    end

    def get_storage_files
      @parts.map do |label, *_|
        storage_name = storage_file_path label
        context.bucket.file storage_name
      end.compact
    end

    def bq_table
      unless defined? @bq_table
        @bq_table = context.dataset.table @definition.get_bq_table_name
      end
      @bq_table
    end

    def recreate_bq_table!
      bq_table.delete if bq_table
      @bq_table = context.dataset.create_table @definition.get_bq_table_name, &@definition.bq_schema
    end

    def start_load_job gcloud_file, **_load_settings
      load_settings = {
          format: 'csv',
          quote: '"',
          delimiter: ';',
          create: 'never',
          write: 'append',
          max_bad_records: 0
      }
      load_settings.merge! _load_settings unless _load_settings.empty?
      bq_table.load gcloud_file, **load_settings
    end

    def self.define **kwargs, &block
      ::ExportToGcloud::Exporter::Definition.set_last_definition self, kwargs, &block
    end

    private

    def compress_file!(original_file)
      err = %x(pigz -f9 #{original_file.to_path} 2>&1)
      compressed_file = Pathname.new "#{original_file.to_path}.gz"
      raise "Compression of #{original_file.to_path} failed: #{err}" unless compressed_file.exist?
      original_file.delete if original_file.exist?
      compressed_file
    end

  end

end

require_relative '../exporter/definition'
require_relative '../exporter/context'