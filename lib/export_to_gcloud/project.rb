

module ExportToGcloud

  class Project

    attr_reader :client,
        :bucket, :storage_prefix,
        :dataset,
        :local_tmp_path,
        :definition_finder


    def initialize project_name, config_hash
      ::ExportToGcloud.require_dependencies
      @client = ::Gcloud.new project_name, config_hash
    end

    def set_bucket name
      @bucket = client.storage.bucket name
    end

    def set_storage_prefix prefix
      @storage_prefix = prefix
    end

    def set_dataset name
      @dataset = client.bigquery.dataset name
    end

    def set_local_tmp_path path
      @local_tmp_path = Pathname.new path
    end

    def set_definition_finder finder
      @definition_finder = finder
    end

    # waits for BigQuery jobs
    # - send a block to do something with failed
    def wait_for_load_jobs(jobs, &block)
      jobs_left = jobs.dup
      failed = []
      sleeper = ->(_retries) {sleep 2 * _retries + 5}
      retries = 0

      until jobs_left.empty?
        sleeper.call retries
        retries += 1
        jobs_left.each &:reload!
        jobs_left.delete_if do |j|
          if j.done?
            failed << {id: j.job_id, error: j.error, sources: j.sources} if j.failed?
            true
          end
        end
      end

      block.call failed unless failed.empty?
    end

    def get_exporter identificator
      identificator = identificator.to_s

      @definitions ||= {}
      unless @definitions.has_key? identificator
        file_path = definition_finder[identificator]
        load file_path
        @definitions[identificator] = ::ExportToGcloud::Exporter.get_last_definition ||
            raise("File #{file_path} must define exporter for '#{identificator}'!")
      end

      definition = @definitions[identificator]
      definition.create_exporter self
    end


  end

end