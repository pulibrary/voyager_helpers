require_relative 'queries'
require_relative 'oracle_connection'

module VoyagerHelpers

  class ChangeReport
    attr_accessor :created, :updated, :deleted

    def initialize
      self.created = []
      self.updated = []
      self.deleted = []
    end

    def all_ids
      [self.created,self.updated,self.deleted].flatten.map{ |h| h[:id]}
    end

    # @return [Array<Hash>] Suitable for processing into a ResourceSync
    #   document. Each Hash will have keys :id, :change, and :lastmod. :change
    #   with be one of :created, :updated, or :deleted. The Array is sorted by
    #   :lastmod (earliest to latest).
    def to_a
      a = []
      [:created, :updated, :deleted].each do |attr_|
        self.send(attr_).each do |h|
          a << { change: attr_ }.merge!(h)
        end
      end
      a.sort_by { |h| h[:lastmod] }
    end

    # DCI??? Should only be called on a bib report to merge in a holding report
    #
    # Note that we only make it possible to pass in bib_info so that we can
    # test w/o an Oracle connection
    def merge_in_holding_report(holding_report, bib_info=nil)
      holding_ids = holding_report.all_ids
      bib_info = self.class.get_bib_ids_for_holding_ids(holding_ids) if bib_info.nil?
      bib_info.each do |bib_hash|
        if bib_hash.has_key?(:id) # else it was deleted
          # if bib id is not in any group (new holding),
          # add to update group (bib record must exist, but wasn't updated)
          holding = holding_report.entry_from_created_updated_by_id(bib_hash[:holding_id])
          if ![created_ids, updated_ids].flatten.include?(bib_hash[:id])
            unless holding.nil? # if it was in holding_report.deleted_ids because it was suppressed
              bib_hash[:lastmod] = holding[:lastmod]
            end
            bib_hash.delete(:holding_id)
            self.updated << bib_hash
          # else if the bib is in the created or ...
          elsif created_ids.include?(bib_hash[:id])
            bib = entry_from_created_by_id(bib_hash[:id])
            unless holding.nil?
              bib[:lastmod] = [bib[:lastmod], holding[:lastmod]].max
            end
            # ... updated group, update the report
          elsif updated_ids.include?(bib_hash[:id])
            bib = entry_from_updated_by_id(bib_hash[:id])
            unless holding.nil?
              bib[:lastmod] = [bib[:lastmod], holding[:lastmod]].max
            end
          end
        end
      end
    end


    def entry_from_created_updated_by_id(id)
      [self.created, self.updated].flatten.select { |e| e[:id] == id }.first
    end


    def created_ids
      self.created.map { |h| h[:id] }
    end

    def updated_ids
      self.updated.map { |h| h[:id] }
    end

    def deleted_ids
      self.deleted.map { |h| h[:id] }
    end

    private

    def entry_from_created_by_id(id)
      self.created.flatten.select { |e| e[:id]  == id }.first
    end

    def entry_from_updated_by_id(id)
      self.updated.flatten.select { |e| e[:id]  == id }.first
    end

    def self.get_bib_ids_for_holding_ids(holding_ids)
      bib_info = []
      connection do |c|
        holding_ids.each do |holding_id|
          bib_h = bib_id_for_holding_id(holding_id, c)
          bib_h[:holding_id] = holding_id
          new_bib_h = bib_h.clone
          bib_h[:id].each do |id|
            new_bib_h[:id] = id
            bib_info << new_bib_h
          end
        end
      end
      bib_info
    end

    class << self

      include VoyagerHelpers::Queries
      include VoyagerHelpers::OracleConnection

      private

      def bib_id_for_holding_id(holding_id, conn=nil)
        if conn.nil?
          connection do |c|
            exec_bib_id_for_holding_id(holding_id, c)
          end
        else
          exec_bib_id_for_holding_id(holding_id, conn)
        end
      end

      def exec_bib_id_for_holding_id(holding_id, connection)
        data = {}
        cursor = connection.parse(VoyagerHelpers::Queries.bib_id_for_holding_id)
        cursor.bind_param(':id', holding_id)
        cursor.exec()
        while row = cursor.fetch
          id = row.shift.to_s
          created = row.shift
          updated = row.shift
          if data[:id]
            data[:id] << id
          else
            data[:id] = [id]
          end
          if !updated.nil?
            data[:lastmod] = updated.to_datetime.new_offset(0)
          elsif !created.nil?
            data[:lastmod] = created.to_datetime.new_offset(0)
          else
            data[:lastmod] = DateTime.now.new_offset(0)
          end
        end
        cursor.close()
        data
      end

    end # class << self

  end # class ChangeReport
end # module VoyagerHelpers
