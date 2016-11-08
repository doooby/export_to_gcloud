
RSpec.describe ETG::Exporter do

  def definition **attrs
    ETG::Exporter::Definition.new ETG::Exporter, {
        name: 'kkk',
        bq_schema: lambda{},
        data: ''
    }.merge!(attrs)
  end

  def context **opts
    ETG::Exporter::Context.new ETG::FakeGcloudClient.new, **opts
  end

  describe '.new' do
    it 'with no parts' do
      exporter = ETG::Exporter.new definition, context
      expect(exporter.instance_variable_get '@parts').to eq([])
    end

    it 'Array of parts' do
      exporter_definition = definition parts: [['kkk', 'black_power']]
      exporter = ETG::Exporter.new exporter_definition, context
      expect(exporter.instance_variable_get '@parts').to eq([['kkk', 'black_power']])
    end

    it 'parts defined with Proc' do
      exporter_definition = definition parts: ->(_exporter){
        _exporter.add_data_part 'black_power', label: 'kkk'
      }
      exporter = ETG::Exporter.new exporter_definition, context
      expect(exporter.instance_variable_get '@parts').to eq([['kkk', 'black_power']])
    end
  end

  describe '#local_file_path' do
    it 'specific part by label' do
      exporter = ETG::Exporter.new definition, context(dump_path: '/tmp')
      value = exporter.local_file_path 'black_power'
      expect(value.class).to eq(Pathname)
      expect(value.to_path).to eq('/tmp/kkk_black_power.csv')
    end

    it 'no label' do
      exporter = ETG::Exporter.new definition, context(dump_path: '/tmp')
      value = exporter.local_file_path nil
      expect(value.class).to eq(Pathname)
      expect(value.to_path).to eq('/tmp/kkk.csv')
    end
  end

  describe '#storage_file_path' do
    it 'prefix in definition' do
      exporter = ETG::Exporter.new definition(storage_prefix: 'tmp/'), context
      expect(exporter.storage_file_path nil).to eq('tmp/kkk.csv')
    end

    it 'prefix in context' do
      exporter = ETG::Exporter.new definition, context(storage_prefix: 'tmp/')
      expect(exporter.storage_file_path nil).to eq('tmp/kkk.csv')
    end

    it 'no prefix' do
      exporter = ETG::Exporter.new definition, context
      expect(exporter.storage_file_path nil).to eq('kkk.csv')
    end
  end

  describe '#add_data_part' do
    it 'without label' do
      exporter = ETG::Exporter.new definition, context
      exporter.add_data_part 'kkk', 666
      expect(exporter.instance_variable_get '@parts').to eq([['1', 'kkk', 666]])
    end

    it 'with label' do
      exporter = ETG::Exporter.new definition, context
      exporter.add_data_part 'kkk', 666, label: 'black_power'
      expect(exporter.instance_variable_get '@parts').to eq([['black_power', 'kkk', 666]])
    end
  end

  describe '#process_all_parts!' do
    it '' do
    end
  end

  describe '#process_part!' do
    it '' do
    end
  end

  describe '#upload_file!' do
    it '' do
    end
  end

  describe '#get_storage_files' do
    it '' do
    end
  end

  describe '#bq_table' do
    it '' do
    end
  end

  describe '#recreate_bq_table!' do
    it '' do
    end
  end

  describe '#start_load_job' do
    it '' do
    end
  end

  describe '.define' do
    it '' do
    end
  end

end