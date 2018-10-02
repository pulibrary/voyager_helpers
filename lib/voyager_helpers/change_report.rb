require_relative 'queries'
require_relative 'oracle_connection'

module VoyagerHelpers

  class ChangeReport
    attr_accessor :updated, :deleted

    def initialize
      self.updated = []
      self.deleted = []
    end

    def all_ids
      [self.updated,self.deleted].flatten
    end

    def updated_ids
      self.updated
    end

    def deleted_ids
      self.deleted
    end

  end # class ChangeReport
end # module VoyagerHelpers
