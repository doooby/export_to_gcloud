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

  describe '#create_data_file!' do
    it '' do
    end
  end

end