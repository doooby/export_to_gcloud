require 'gcloud/bigquery'

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
      "#{prefix}#{label}"
    end

    def add_data_part *args, label:nil
      args.unshift(label ? label.to_s : (@parts.length+1).to_s)
      @parts << args
    end

    def process_all_parts!
      add_data_part label: 'all' if @parts.empty?

      @parts.map do |label, *part_args|
        file = local_file_path label
        storage_name = storage_file_path label

        create_data_file! file, *part_args
        gcloud_file = upload_file! file, storage_name
        start_load_job gcloud_file
      end
    end

    def create_data_file! file_path, *part_data
      File.write file_path, @definition.get_data(*part_data)
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



    class Definition < OpenStruct

      def initialize exporter_type, def_attrs
        self.type = exporter_type
        super def_attrs
      end

      def create_exporter project
        type.new self, project
      end

      def validate!
        (String === name && !name.empty?)   || raise('`name` must be defined!')
        Proc === bq_schema                  || raise('`bq_schema` must be defined as a Proc!')
        data                                || raise('`data` must be defined!')
        type.validate_definition! if type.respond_to? 'validate_definition!'
      end

      def get_data *args
        Proc === data ? data[*args] : data
      end

    end



    def self.define **kwargs
      last_definition = ::ExportToGcloud::Exporter::Definition.new self, kwargs
      yield last_definition
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