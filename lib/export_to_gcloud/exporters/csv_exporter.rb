
module ExportToGcloud

  class CSVExporter < Exporter

    private

    def create_data_file! file, *part_data
      data = @definition.get_data(*part_data)

      csv_data = CSV.generate col_sep: ';', force_quotes: false do |csv|
        data.each{|row| csv << row}
      end

      File.write file.to_path, csv_data
    end

  end

end