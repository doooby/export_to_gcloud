

module ExportToGcloud

  class Project

    attr_reader :client, :definition_finder


    def initialize project_name, config_file
      ::ExportToGcloud.require_dependencies
      @client = ::Gcloud.new project_name, config_file
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

    def get_exporter name
      name = name.to_s

      @definitions ||= {}
      unless @definitions.has_key? name
        @definitions[name] = load_definition name
      end

      definition = @definitions[name]
      definition.create_exporter
    end
    
    private
    
    def load_definition name
      file_path = definition_finder[name]
      load file_path
      definition = ::ExportToGcloud::Exporter.get_last_definition

      unless definition
        raise("File #{file_path.to_s} must define exporter for '#{name}'!")
      end

      unless definition.name == name
        raise "File #{file_path.to_s} defines '#{definition.name}' instead of '#{name}'"
      end

      definition
    end

  end

end