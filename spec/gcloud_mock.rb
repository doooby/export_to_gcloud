
module ExportToGcloud

  TEST_ENV = true

end

ETG = ExportToGcloud

class ETG::FakeGcloudClient

  def storage
    Storage.new
  end

  def bigquery
    Storage.new
  end

  class Storage

    def bucket name
      Bucket.new name
    end

    class Bucket < Struct.new(:name)

    end

  end

  class Storage

    def dataset key
      Dataset.new key
    end

    class Dataset < Struct.new(:key)

    end

  end

end



