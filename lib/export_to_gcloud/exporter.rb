
module ExportToGcloud

  class Exporter

    def initialize definition, project
      @definition = definition
      @project = project
      @parts = []
    end

    def local_file_path label
      @project.local_tmp_path.join "#{@definition.name}_#{label}.csv"
    end

    def storage_file_path label
      prefix = @definition.storage_prefix || @project.storage_prefix
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
      gcloud_file = @project.bucket.create_file file, storage_name, chunk_size: 2**21 # 2MB
      file.delete
      gcloud_file
    end

    def get_storage_files
      @parts.map do |label, *_|
        storage_name = storage_file_path label
        @project.bucket.file storage_name
      end.compact
    end

    def bq_table
      unless defined? @bq_table
        @bq_table = @project.dataset.table @definition.get_bq_table_name
      end
      @bq_table
    end

    def recreate_bq_table!
      bq_table.delete if bq_table
      @bq_table = @project.dataset.create_table @definition.get_bq_table_name, &@definition.bq_schema
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

    class Definition < OpenStruct

      def initialize exporter_type, def_attrs
        super def_attrs.merge!(type: exporter_type)
      end

      def create_exporter project
        type.new self, project
      end

      def validate!
        (String === name && !name.empty?)   || raise('`name` must be defined!')
        Proc === bq_schema                  || raise('`bq_schema` must be defined as a Proc!')
        data                                || raise('`data` must be defined!')
        type.validate_definition! self if type.respond_to? 'validate_definition!'
      end

      def get_data *args
        Proc === data ? data[*args] : data
      end

      def get_bq_table_name
        bq_table_name || name
      end

    end



    def self.define **kwargs
      last_definition = ::ExportToGcloud::Exporter::Definition.new self, kwargs
      yield last_definition if block_given?
      last_definition.validate!
      ::ExportToGcloud::Exporter.set_last_definition last_definition
    end

    def self.get_last_definition
      definition = @last_definition
      @last_definition = nil
      definition
    end

    private

    def compress_file!(original_file)
      err = %x(pigz -f9 #{original_file.to_path} 2>&1)
      compressed_file = Pathname.new "#{original_file.to_path}.gz"
      raise "Compression of #{original_file.to_path} failed: #{err}" unless compressed_file.exist?
      original_file.delete if original_file.exist?
      compressed_file
    end

    def self.set_last_definition definition
      @last_definition = definition
    end

  end

end