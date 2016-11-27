
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

  def load_opts **custom
    opts = {
        format: 'csv',
        quote: '"',
        delimiter: ';',
        create: 'never',
        write: 'append',
        max_bad_records: 0
    }
    opts = custom.merge opts unless custom.empty?
    opts
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
    it 'no parts defined without recreation' do
      exporter = ETG::Exporter.new definition, context
      expect(exporter).not_to receive(:recreate_bq_table!)
      expect(exporter).to receive(:process_part!).with(['all'])
      exporter.process_all_parts! false
    end

    it 'defined parts without recreation' do
      exporter = ETG::Exporter.new definition, context
      exporter.instance_variable_set '@parts', [['1', 'kkk'], ['2', 666]]
      expect(exporter).not_to receive(:recreate_bq_table!)
      expect(exporter).to receive(:process_part!).once.with(['1', 'kkk'])
      expect(exporter).to receive(:process_part!).once.with(['2', 666])
      exporter.process_all_parts! false
    end

    it 'single defined part & recreation' do
      exporter = ETG::Exporter.new definition, context
      exporter.instance_variable_set '@parts', [['1', 'kkk']]
      expect(exporter).to receive(:recreate_bq_table!)
      expect(exporter).to receive(:process_part!).once.with(['1', 'kkk'])
      exporter.process_all_parts! true
    end
  end

  describe '#process_part!' do
    it 'without any args' do
      exporter = ETG::Exporter.new definition, context
      expect(exporter).to receive(:local_file_path).with('1').and_return('/1.csv')
      expect(exporter).to receive(:create_data_file!).with('/1.csv')
      expect(exporter).to receive(:storage_file_path).with('1').and_return('1.csv')
      expect(exporter).to receive(:upload_file!).with('/1.csv', '1.csv').and_return('gfile')
      expect(exporter).to receive(:start_load_job).with('gfile').and_return('job')
      expect(exporter.process_part! '1').to eq('job')
    end

    it 'with two args' do
      exporter = ETG::Exporter.new definition, context
      expect(exporter).to receive(:local_file_path).with('1').and_return('/1.csv')
      expect(exporter).to receive(:create_data_file!).with('/1.csv', 'kkk', 666)
      expect(exporter).to receive(:storage_file_path).with('1').and_return('1.csv')
      expect(exporter).to receive(:upload_file!).with('/1.csv', '1.csv').and_return('gfile')
      expect(exporter).to receive(:start_load_job).with('gfile').and_return('job')
      expect(exporter.process_part! '1', 'kkk', 666).to eq('job')
    end
  end

  describe '#upload_file!' do
    it 'upload' do
      exporter = ETG::Exporter.new definition, context(bucket: 'b')
      file = ETG.define_fake_object.new
      expect(exporter).to receive(:compress_file!).with('file').and_return(file)
      expect(file).to receive(:delete).once
      result = exporter.upload_file! 'file', 'gfile'
      expect(result).to be_a(Hash)
      expect(result[:method]).to eq(:create_file)
      expect(result[:args]).to eq([file, 'gfile', {chunk_size: 2**21}])
    end
  end

  describe '#get_storage_files' do
    it 'no parts defined' do
      exporter = ETG::Exporter.new definition, context
      expect(exporter.get_storage_files).to eq([])
    end

    it 'single existing part - file call not mocked' do
      exporter = ETG::Exporter.new definition(parts: [%w[all]]), context(bucket: 'b')
      expect(exporter).to receive(:storage_file_path).with('all').and_return('all.csv')
      bucket = exporter.instance_variable_get(:@context).bucket
      expect(exporter.get_storage_files).to eq([{object: bucket, method: :file, args: ['all.csv']}])
    end

    it 'two parts, on not uploaded' do
      exporter = ETG::Exporter.new definition(parts: [%w[1], %w[2]]), context(bucket: 'b')
      expect(exporter).to receive(:storage_file_path).once.with('1').and_return('1.csv')
      expect(exporter).to receive(:storage_file_path).once.with('2').and_return('2.csv')
      bucket = exporter.instance_variable_get(:@context).bucket
      expect(bucket).to receive(:file).once.with('1.csv').and_return(nil)
      expect(bucket).to receive(:file).once.with('2.csv').and_return('gfile_2.csv')
      expect(exporter.get_storage_files).to eq(['gfile_2.csv'])
    end
  end

  describe '#bq_table' do
    it 'already defined' do
      _definition = definition
      exporter = ETG::Exporter.new _definition, context
      exporter.instance_variable_set :@bq_table, 'table'
      expect(_definition).not_to receive(:get_bq_table_name)
      expect(exporter.bq_table).to eq('table')
    end

    it 'not defined yet' do
      _definition = definition
      exporter = ETG::Exporter.new _definition, context(dataset: 'ds')
      expect(_definition).to receive(:get_bq_table_name).and_return('kkk')
      result = exporter.bq_table
      expect(result).to be_a(Hash)
      expect(result[:method]).to eq(:table)
      expect(result[:args]).to eq(['kkk'])
    end
  end

  describe '#recreate_bq_table!' do
    it 'with delete' do
      _definition = definition bq_table_name: 'kkk', bq_schema: ->(){}
      _context = context dataset: 'ds'
      exporter = ETG::Exporter.new _definition, _context

      table = ETG.define_fake_object.new
      exporter.instance_variable_set :@bq_table, table
      expect(table).to receive(:delete)

      dataset = _context.dataset
      expect(dataset).to receive(:create_table).with('kkk'){|*_, &block|
        expect(block).to eq(_definition.bq_schema)
      }.and_return('new_table')

      expect(exporter.recreate_bq_table!).to eq('new_table')
    end
  end

  describe '#start_load_job' do
    it 'defaults' do
      exporter = ETG::Exporter.new definition, context
      table = ETG.define_fake_object{def load(*_); end}.new
      exporter.instance_variable_set :@bq_table, table

      expect(table).to receive(:load).with('gfile', load_opts).and_return('load_job')
      expect(exporter.start_load_job 'gfile').to eq('load_job')
    end
  end

end