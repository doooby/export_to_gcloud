
module ExportToGcloud

  TEST_ENV = true

end

ETG = ExportToGcloud

def ETG.define_fake_object *attrs, &custom_definitions
  klass = attrs.empty? ? Class.new : Struct.new(*attrs)
  klass.class_eval do
    def method_missing name, *args; {object: self, method: name.to_sym, args: args}; end
    def respond_to? *_; true; end
    # def respond_to_missing? *_; true; end
  end
  klass.class_eval &custom_definitions if custom_definitions
  klass
end

class ETG::FakeGcloudClient

  def storage
    Storage.new
  end

  def bigquery
    BigQuery.new
  end

  class Storage

    Bucket = ETG.define_fake_object :name

    def bucket name
      Bucket.new name
    end

  end

  class BigQuery

    Dataset = ETG.define_fake_object :key

    def dataset key
      Dataset.new key
    end

  end

end



