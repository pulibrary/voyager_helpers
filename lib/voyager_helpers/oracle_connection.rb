module VoyagerHelpers
  module OracleConnection

    def connection(conn=nil)
      if conn.nil?
        begin
          conn = OCI8.new(
            VoyagerHelpers.config.db_user,
            VoyagerHelpers.config.db_password,
            VoyagerHelpers.config.db_name
          )
          yield conn
        rescue NameError
          return if ENV['CI']
        ensure
          conn.logoff unless conn.nil?
        end
      else
        yield conn
      end
    end

  end # module Connection
end # module VoyagerHelpers
