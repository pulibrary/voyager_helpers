require_relative 'queries'
require_relative 'oracle_connection'
require 'date'
require 'diffy'

module VoyagerHelpers
  class SyncFu
    class << self
      include VoyagerHelpers::Queries
      include VoyagerHelpers::OracleConnection
      # @param earlier_file [String]
      # @param later_file [String]
      #
      # Both files are formatted with a with one line per combination
      # of bib ID and holding ID separated by ' ', e.g.:
      # separated by ' ', e.g.:
      #
      #  ```
      #  2 2
      #  3 3
      #  3 233918
      #  4 4
      #  5 5
      #  ```
      #
      # These files can be obtained by calling #bibs_with_holdings_to_file
      # @return [ChangeReport]
      def compare_id_dumps(earlier_file, later_file)
        diff = Diffy::Diff.new(earlier_file, later_file, source: 'files')
        diff_hashes = diff_to_hash_array(diff)
        grouped_diffs = group_by_plusminus(diff_hashes)
        grouped_diffs_to_change_report(grouped_diffs)
      end

      def bibs_with_holdings_to_file(file_handle, conn=nil)
        query = VoyagerHelpers::Queries.all_unsuppressed_bibs_with_holdings
        merged_ids_to_file(file_handle, query, conn)
      end

      def bib_ids_to_file(file_handle, conn=nil)
        query = VoyagerHelpers::Queries.all_unsuppressed_bibs_with_holdings
        exec_bib_ids_to_file(query, file_handle, conn)
      end

      ## For Recap Processing
      ## Should come in as yyyy-mm-dd hh24:mi:ss.ffffff - 0400
      def recap_barcodes_since(last_dump_date)
        VoyagerHelpers::Liberator.updated_recap_barcodes(last_dump_date.strftime("%Y-%m-%d %H:%M:%S.%6N %z"))
      end

      private

      def merged_ids_to_file(file_handle, query, conn=nil)
        connection(conn) do |c|
          exec_merged_ids_to_file(query, file_handle, c)
        end
      end

      def exec_bib_ids_to_file(query, file_handle, conn)
        connection(conn) do |c|
          bibs = Set.new
          cursor = conn.parse(query)
          cursor.exec
          while row = cursor.fetch
            bibs << row[0]
          end
          cursor.close
          File.open(file_handle, 'w') do |f|
            bibs.each do |id|
              f.puts(id)
            end
          end
        end
      end

      def exec_merged_ids_to_file(query, file_handle, conn)
        cursor = conn.parse(query)
        cursor.exec
        File.open(file_handle, 'w') do |f|
          while row = cursor.fetch
            bib_id = row[0]
            holding_id = row[1]
            f.puts("#{bib_id} #{holding_id}")
          end
        end
        cursor.close
      end

      def parse_merged_diff_line_to_hash(line)
        parts = line.split(' ')
        parts[0] = ' ' + parts[0] if parts[0] =~ /^[0-9]/
        { plusminus: parts[0][0], bib_id: parts[0][1..-1] }
      end

      def diff_to_hash_array(diff)
        diff.to_a.map { |line| parse_merged_diff_line_to_hash(line) }
      end

      def group_by_plusminus(diff_hashes)
        grouped = diff_hashes.group_by { |h| h[:plusminus] }
        same = Set.new
        minuses = Set.new
        pluses = Set.new
        same += grouped[' '].map { |hash| hash[:bib_id] }
        minuses += grouped['-'].map { |hash| hash[:bib_id] }
        pluses += grouped['+'].map { |hash| hash[:bib_id] }
        deletes = minuses - pluses - same
        updates = pluses + minuses - deletes
        { updates: updates.to_a, deletes: deletes.to_a }
      end

      def grouped_diffs_to_change_report(grouped_diffs)
        report = ChangeReport.new
        report.updated += grouped_diffs[:updates]
        report.deleted += grouped_diffs[:deletes]
        report
      end
    end # class << self
  end # class SyncFu
end # module VoyagerHelpers











