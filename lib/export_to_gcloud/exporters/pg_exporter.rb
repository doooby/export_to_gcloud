
module ExportToGcloud

  class PGExporter < Exporter

    def create_data_file! file, *part_data
      sql = @definition.get_data(*part_data)

      schema = ::Gcloud::Bigquery::Table::Schema.new nil
      @definition.bq_schema.call schema
      string_fields = schema.fields.select{|f| f['type']=='STRING'}.map{|f| f['name']}

      force_quote = if string_fields.empty?
        ''
      else
        ", FORCE_QUOTE (#{string_fields.join ', '})"
      end
      sql = "COPY (#{sql}) TO '#{file.to_path}' WITH (FORMAT CSV, DELIMITER ';', QUOTE '\"'#{force_quote});"

      @definition.sql_executor sql
    end
    
    def self.validate_definition! definition
      definition.sql_executor || raise('`sql_executor` needs to be defined!')
    end

  end

end