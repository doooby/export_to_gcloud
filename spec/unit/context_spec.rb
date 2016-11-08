
RSpec.describe ETG::Exporter::Context do

  def create_context **attrs
    ETG::Exporter::Context.new ETG::FakeGcloudClient.new, **attrs
  end

  # describe '#set' do
  #
  # end

  describe '#set_dump_path' do
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
  end

  describe '#set_storage_prefix' do
    it 'sets storage_prefix' do
      context = create_context storage_prefix: 'export/'
      expect(context.storage_prefix).to eq('export/')
    end
  end

  describe '#set_bucket' do
    it 'sets bucket from string' do
      context = create_context bucket: 'export'
      expect(context.bucket.class).to eq(ETG::FakeGcloudClient::Storage::Bucket)
      expect(context.bucket.name).to eq('export')
    end

    it 'sets bucket as object' do
      context = create_context bucket: ETG::FakeGcloudClient::Storage::Bucket.new('export')
      expect(context.bucket.class).to eq(ETG::FakeGcloudClient::Storage::Bucket)
      expect(context.bucket.name).to eq('export')
    end
  end

  describe '#set_dataset' do
    it 'sets dataset from string' do
      context = create_context dataset: 'export'
      expect(context.dataset.class).to eq(ETG::FakeGcloudClient::Storage::Dataset)
      expect(context.dataset.key).to eq('export')
    end

    it 'sets dataset as object' do
      context = create_context dataset: ETG::FakeGcloudClient::Storage::Dataset.new('export')
      expect(context.dataset.class).to eq(ETG::FakeGcloudClient::Storage::Dataset)
      expect(context.dataset.key).to eq('export')
    end
  end

  describe '#copy' do
    it 'copies itself' do
      context1 = create_context storage_prefix: 'export/', bucket: 'export'
      context2 = context1.copy
      expect(context1.storage_prefix).to eq(context2.storage_prefix)
      expect(context1.bucket).to eq(context2.bucket)
    end
  end

end