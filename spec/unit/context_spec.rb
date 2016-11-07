
RSpec.describe ETG::Exporter::Context do

  def create_context **attrs
    ETG::Exporter::Context.new ETG::FakeGcloudClient.new, **attrs
  end

  describe '#set' do

    it 'sets dump_path as String' do
      context = create_context dump_path: '/tmp'
      expect(context.dump_path.class).to eq(Pathname)
      expect(context.dump_path.to_path).to eq('/tmp')
    end

    it 'sets dump_path as Pathname' do
      context = create_context dump_path: Pathname.new('/tmp')
      expect(context.dump_path.class).to eq(Pathname)
      expect(context.dump_path.to_path).to eq('/tmp')
    end

    it 'sets storage_prefix' do
      context = create_context storage_prefix: 'export/'
      expect(context.storage_prefix).to eq('export/')
    end

  end

end