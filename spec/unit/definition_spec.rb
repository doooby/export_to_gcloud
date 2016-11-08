
RSpec.describe ETG::Exporter::Definition do

  def create_definition exporter_type=nil, **attrs
    exporter_type = case exporter_type
      when 'csv' then ETG::CSVExporter
      when 'pg' then ETG::PGExporter
      else ETG::Exporter
    end
    ETG::Exporter::Definition.new exporter_type, attrs
  end

  def valid_definition exporter_type=nil, **changes
    attrs = {
        name: 'kkk',
        bq_schema: lambda{},
        data: 'data'
    }.merge! changes
    create_definition exporter_type, **attrs
  end

  describe '#validate!' do
    it 'invalid name' do
      definition = valid_definition name: nil
      expect{definition.validate!}.to raise_error('`name` must be defined!')
    end

    it 'invalid bq_schema' do
      definition = valid_definition bq_schema: nil
      expect{definition.validate!}.to raise_error('`bq_schema` must be defined as a Proc!')
    end

    it 'invalid data' do
      definition = valid_definition data: nil
      expect{definition.validate!}.to raise_error('`data` must be defined!')
    end

    it 'valid definition - exporter' do
      expect{valid_definition.validate!}.not_to raise_error
    end

    it 'valid definition - csv exporter' do
      expect{valid_definition('csv').validate!}.not_to raise_error
    end

    it 'valid definition - pg exporter' do
      definition = valid_definition get_sql_executor: lambda{}
      expect{definition.validate!}.not_to raise_error
    end

    it 'valid definition - pg exporter default executor' do
      ETG::PGExporter.default_executor = lambda{}
      expect{valid_definition('pg').validate!}.not_to raise_error
    end
  end

  describe '#get_data' do
    it 'raw data' do
      definition = create_definition data: 'kkk'
      expect(definition.get_data).to eq('kkk')
    end

    it 'Proc data with args' do
      definition = create_definition data: -> (arg1, arg2) { "#{arg1}_#{arg2}" }
      expect(definition.get_data 'kkk', 666).to eq('kkk_666')
    end
  end

  describe '#get_bq_table_name' do
    it 'implicit name' do
      definition = create_definition name: 'kkk'
      expect(definition.get_bq_table_name).to eq('kkk')
    end

    it 'explicit name' do
      definition = create_definition name: 'kkk', bq_table_name: 'black_power'
      expect(definition.get_bq_table_name).to eq('black_power')
    end
  end

end