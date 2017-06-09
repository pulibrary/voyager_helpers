require 'marc'
require 'time'
require_relative 'queries'
require_relative 'oracle_connection'
require_relative 'course'
require_relative 'course_bib'
require 'oci8' unless ENV['CI']

module VoyagerHelpers
  class Liberator
    class << self
      include VoyagerHelpers::Queries
      include VoyagerHelpers::OracleConnection

      # @param bib_id [Fixnum] A bib record id
      # @option opts [Boolean] :holdings (true) Include holdings?
      # @option opts [Boolean] :holdings_in_bib (true) Copy 852 fields to the bib record?
      # @return [MARC::Record] If `holdings: false` or there are no holdings.
      # @return [Array<MARC::Record>] If `holdings: true` (default) and there
      #   are holdings.
      def get_bib_record(bib_id, conn=nil, opts={})
        connection(conn) do |c|
          unless bib_is_suppressed?(bib_id, c)
            if opts.fetch(:holdings, true)
              get_bib_with_holdings(bib_id, c, opts)
            else
              get_bib_without_holdings(bib_id, c)
            end
          end
        end
      end

      def get_bib_update_date(bib_id, conn=nil)
        query = VoyagerHelpers::Queries.bib_update_date
        connection(conn) do |c|
          cursor = c.parse(query)
          cursor.bind_param(':bib_id', bib_id)
          cursor.exec()
          date = cursor.fetch
          cursor.close()
          date
        end
      end

      def get_mfhd_update_date(mfhd_id, conn=nil)
        query = VoyagerHelpers::Queries.mfhd_update_date
        connection(conn) do |c|
          cursor = c.parse(query)
          cursor.bind_param(':mfhd_id', mfhd_id)
          cursor.exec()
          date = cursor.fetch
          cursor.close()
          date
        end
      end

      # @param mfhd_id [Fixnum] A holding record id
      # @return [MARC::Record]
      def get_holding_record(mfhd_id, conn=nil)
        connection(conn) do |c|
          unless mfhd_is_suppressed?(mfhd_id, c)
            segments = get_mfhd_segments(mfhd_id, c)
            MARC::Reader.decode(segments.join(''), :external_encoding => "UTF-8", :invalid => :replace, :replace => '') unless segments.empty?
          end
        end
      end

      # @param bib_id [Fixnum] A bib record id
      # @return [Array<MARC::Record>]
      def get_holding_records(bib_id, conn=nil)
        records = []
        connection(conn) do |c|
          get_bib_mfhd_ids(bib_id, c).each do |mfhd_id|
            record = get_holding_record(mfhd_id, c)
            records << record unless record.nil?
          end
        end
        records
      end

      # strips invalid xml characters to prevent parsing errors
      # only used for "cleaning" individually retrieved records
      def valid_xml(xml_string)
        invalid_xml_range = /[^\u0009\u000A\u000D\u0020-\uD7FF\uE000-\uFFFD]/
        xml_string.gsub(invalid_xml_range, '')
      end

      # @return [<Hash>]
      def get_item_statuses
        query = VoyagerHelpers::Queries.statuses
        statuses = {}
        connection do |c|
          c.exec(query) { |id,desc| statuses.store(id,desc) }
        end
        statuses
      end

      def get_items_for_holding(mfhd_id, conn=nil)
        connection(conn) do |c|
          accumulate_items_for_holding(mfhd_id, c)
        end
      end

      def get_item(item_id, conn=nil)
        connection(conn) do |c|
          get_info_for_item(item_id, c)
        end
      end

      def get_locations
        query = VoyagerHelpers::Queries.all_locations
        locations = {}
        connection do |c|
          c.exec(query) do |id, code, display_name, suppress|
            locations[id] = {}
            locations[id][:code] = code
            locations[id][:display_name] = display_name
            locations[id][:suppress] = suppress
          end
        end
        locations
      end

      # This fires off quite a few queries; could probably be optimized
      def get_items_for_bib(bib_id)
        connection do |c|
          items = []
          mfhds = get_holding_records(bib_id, c)
          mfhds.each do |mfhd|
            mfhd_hash = mfhd.to_hash
            mfhd_id = id_from_mfhd_hash(mfhd_hash)
            holding_items = get_items_for_holding(mfhd_id, c)
            unless holding_items.empty?
              any_items = true
              data = { holding_id: mfhd_id.to_i }
              # Everyone seems quite sure that we don't repeat 852 per mfhd
              field_852 = fields_from_marc_hash(mfhd_hash, '852').first['852']
              data[:perm_location] = location_from_852(field_852)
              data[:call_number] = callno_from_852(field_852)
              notes = holdings_notes_from_mfhd_hash(mfhd_hash)
              data[:notes] = notes unless notes.empty?
              data[:items] = []
              holding_items.each do |item|
                data[:items] << item
              end
              data[:items].sort_by! { |i| i[:item_sequence_number] || 0 }.reverse!
              items << data
            end
          end
          group_items(items)
        end
      end

      # @param bibs [Array<Fixnum>] Bib ids
      # @param full [Boolean] true return full availability for single bib, false (default) first 2 holdings
      # @return [Hash] :bib_id_value => [Hash] bib availability
      #
      #
      # Bib availability hash:
      # For the bib's holding records:
      # :holding_id_value => [Hash] holding availability
      #
      # Holding availability hash:
      # :status => [String] Voyager item status for the first item.
      # :location => [String] Holding location code (mainly for debugging)
      # :more_items => [Boolean] Does the holding record have more than 1 item?
      def get_availability(bibs, full=false)
        number_of_mfhds = full ? 0..-1 : 0..1 # all vs first 2
        connection do |c|
          availability = {}
          bibs.each do |bib_id|
            availability[bib_id] = {}
            mfhds = get_holding_records(bib_id, c)
            mfhds[number_of_mfhds].each do |mfhd|
              mfhd_hash = mfhd.to_hash
              mfhd_id = id_from_mfhd_hash(mfhd_hash)
              field_852 = fields_from_marc_hash(mfhd_hash, '852').first['852']
              field_852 = location_from_852(field_852)
              holding_item_ids = get_item_ids_for_holding(mfhd_id, c)

              availability[bib_id][mfhd_id] = {} # holding record availability hash
              availability[bib_id][mfhd_id][:more_items] = holding_item_ids.count > 1
              availability[bib_id][mfhd_id][:location] = field_852

              availability[bib_id][mfhd_id][:status] = if holding_item_ids.empty?
                if !(order_status = get_order_status(mfhd_id)).nil?
                  order_status
                elsif field_852[/^elf/]
                  'Online'
                else
                  'On Shelf'
                end
              else
                item = get_info_for_item(holding_item_ids.first, c, false)
                unless item[:temp_location].nil?
                  availability[bib_id][mfhd_id][:temp_loc] = item[:temp_location]
                  availability[bib_id][mfhd_id][:course_reserves] = get_courses(holding_item_ids, c).map(&:to_h)
                end
                availability[bib_id][mfhd_id][:copy_number] = item[:copy_number]
                availability[bib_id][mfhd_id][:item_id] = item[:id]
                availability[bib_id][mfhd_id][:on_reserve] = item[:on_reserve]
                due_date = format_due_date(item[:due_date], item[:on_reserve])
                availability[bib_id][mfhd_id][:due_date] = due_date unless due_date.nil?
                item[:status]
              end
            end
          end
          _, availability = availability.first if full # return just holding availability hash (single bib)
          availability
        end
      end

      def get_courses(item_ids, conn = nil)
        courses = []
        query = VoyagerHelpers::Queries.courses_for_reserved_items(item_ids)
        connection(conn) do |c|
          c.exec(query, *item_ids) do |enum|
            reserve_list_id = enum.shift
            department_name = enum.shift
            department_code = enum.shift
            course_name = enum.shift
            course_name = valid_codepoints(course_name) unless course_name.nil?
            course_number = enum.shift
            section_id = enum.shift
            first_name = enum.shift
            first_name = valid_codepoints(first_name) unless first_name.nil?
            last_name = enum.shift
            last_name = valid_codepoints(last_name) unless last_name.nil?
            courses << Course.new(reserve_list_id, department_name, department_code, course_name, course_number, section_id, first_name, last_name)
          end
        end
        courses
      end

      # @param mfhd_id [Fixnum] get info for all mfhd items
      # @return [Array<Hash>] Item hash includes status, enumeration, reserve location code
      def get_full_mfhd_availability(mfhd_id)
        item_availability = []
        items = get_items_for_holding(mfhd_id)
        items.each do |item|
          item_hash = {}
          item_hash[:barcode] = item[:barcode]
          item_hash[:id] = item[:id]
          item_hash[:location] = item[:perm_location]
          item_hash[:temp_loc] = item[:temp_location] unless item[:temp_location].nil?
          item_hash[:copy_number] = item[:copy_number]
          item_hash[:item_sequence_number] = item[:item_sequence_number]
          item_hash[:status] = item[:status]
          item_hash[:on_reserve] = item[:on_reserve] unless item[:on_reserve].nil?
          due_date = format_due_date(item[:due_date], item[:on_reserve])
          item_hash[:due_date] = due_date unless due_date.nil?
          unless item[:enum].nil?
            item_hash[:enum] = item[:enum]
            enum = item[:enum]
            unless item[:chron].nil?
              enum = enum + " (#{item[:chron]})"
              item_hash[:chron] = item[:chron]
            end
            item_hash[:enum_display] = enum
          end
          item_availability << item_hash
        end
        item_availability.sort_by { |i| i[:item_sequence_number] || 0 }.reverse
      end

      # @param mfhd_id [Fixnum] get current issues for mfhd
      # @return [Array<Hash>] Current issues
      def get_current_issues(mfhd_id)
        issues = []
        connection do |c|
          cursor = c.parse(VoyagerHelpers::Queries.current_periodicals)
          cursor.bind_param(':mfhd_id', mfhd_id)
          cursor.exec()
          while enum = cursor.fetch
            issues << enum
          end
          cursor.close()
        end
        issues
      end

      def active_courses
        query = VoyagerHelpers::Queries.active_courses
        courses = []
        connection do |c|
          c.exec(query) do |enum|
            reserve_list_id = enum.shift
            department_name = enum.shift
            department_code = enum.shift
            course_name = enum.shift
            course_name = valid_codepoints(course_name) unless course_name.nil?
            course_number = enum.shift
            section_id = enum.shift
            first_name = enum.shift
            first_name = valid_codepoints(first_name) unless first_name.nil?
            last_name = enum.shift
            last_name = valid_codepoints(last_name) unless last_name.nil?
            courses << Course.new(reserve_list_id, department_name, department_code, course_name, course_number, section_id, first_name, last_name)
          end
        end
        courses
      end

      def course_bibs(reserve_id)
        reserve_ids = Array(reserve_id)
        query = VoyagerHelpers::Queries.course_bibs(reserve_ids)
        courses = []
        connection do |c|
          c.exec(query, *reserve_ids) do |enum|
            courses << CourseBib.new(*enum)
          end
        end
        courses
      end

      def dump_bibs_to_file(ids, file_name, opts={})
        writer = MARC::XMLWriter.new(file_name)
        connection do |c|
          ids.each do |id|
            r = VoyagerHelpers::Liberator.get_bib_record(id, c)
            writer.write(r) unless r.nil?
          end
        end
        writer.close()
      end

      def dump_merged_records_to_file(barcodes, file_name, recap=false)
        writer = MARC::XMLWriter.new(file_name)
        connection do |c|
          barcodes.each do |barcode|
            records = VoyagerHelpers::Liberator.get_records_from_barcode(barcode, recap)
            records.each do |record|
              writer.write(record) unless record.nil?
            end
          end
        end
        writer.close()
      end

      # @param patron_id [String] Either a netID, PUID, or PU Barcode
      # @return [<Hash>]
      def get_patron_info(patron_id)
        id_type = determine_id_type(patron_id)
        query = VoyagerHelpers::Queries.patron_info(id_type)
        connection do |c|
          exec_get_info_for_patron(query, patron_id, c)
        end
      end

      # @param patron_id [String] Either a netID, PUID, or PU Barcode
      # @return [Array<Hash>] Patron Statistical Categories with one key: :stat_code.
      def get_patron_stat_codes(patron_id)
        id_type = determine_id_type(patron_id)
        query = VoyagerHelpers::Queries.patron_stat_codes(id_type)
        connection do |c|
          exec_get_patron_stat_codes(query, patron_id, c)
        end
      end

      # @param mfhd_id [Fixnum] Find order status for provided mfhd ID
      # @return [String] on-order status message and date of status if the status code in whitelist
      # if code is not whitelisted return nil
      def get_order_status(mfhd_id)
        status = nil
        unless (orders = get_orders(mfhd_id)).empty?
          latest_order = orders.max { |a,b| a[:date] <=> b[:date] }
          po_status, li_status = latest_order[:po_status], latest_order[:li_status]
          if on_order?(po_status, li_status)
            status = if li_status == li_rec_complete
              'Order Received'
            elsif li_status == li_pending
              'Pending Order'
            else
              'On-Order'
            end
            status << " #{latest_order[:date].strftime('%m-%d-%Y')}" unless latest_order[:date].nil?
          end
        end
        status
      end

      # @param barcode [String] An item barcode
      # @return [Array<MARC::Record>]
      def get_records_from_barcode(barcode, recap=false)
        records = []
        connection do |c|
          record_ids = get_record_ids_from_barcode(barcode, c, recap)
          record_ids.each do |row|
            bib_id, mfhd_id, item_id = row
            records << single_record_from_barcode(bib_id, mfhd_id, item_id, recap, c)
          end
        end
        records
      end

      # @param date [String] in format yyyy-mm-dd hh24:mi:ss.ffffff timezone_hourtimezone_minute (e.g., 2017-04-05 13:50:25.213245 -0400)
      # @return [Array]
      def updated_recap_barcodes(date)
        barcodes = []
        query = VoyagerHelpers::Queries.recap_update_bib_barcodes
        connection do |c|
          cursor = c.parse(query)
          cursor.bind_param(':last_diff_date', date)
          cursor.exec()
          while row = cursor.fetch
            barcodes << row.first
          end
          cursor.close()
          query = VoyagerHelpers::Queries.recap_update_holding_barcodes
          cursor = c.parse(query)
          cursor.bind_param(':last_diff_date', date)
          cursor.exec()
          while row = cursor.fetch
            barcodes << row.first
          end
          cursor.close()
          query = VoyagerHelpers::Queries.recap_update_item_barcodes
          cursor = c.parse(query)
          cursor.bind_param(':last_diff_date', date)
          cursor.exec()
          while row = cursor.fetch
            barcodes << row.first
          end
          cursor.close()
        end
        barcodes.uniq
      end

      private

      def on_order?(po_status, li_status)
        po_pending = 0
        po_approved = 1
        po_rec_partial = 3
        po_rec_complete = 4
        po_complete = 5
        po_status_whitelist = [po_pending, po_approved, po_rec_partial, po_rec_complete]
        li_approved = 8
        li_rec_partial = 9
        li_status_whitelist = [li_pending, li_rec_complete, li_approved, li_rec_partial]
        (po_status_whitelist.include?(po_status) or li_status_whitelist.include?(li_status)) and
          po_status != po_complete
      end

      def li_pending
        0
      end

      def li_rec_complete
        1
      end

      def group_items(data_arr)
        hsh = data_arr.group_by { |holding| holding[:perm_location] }
        hsh.each do |location, holding_arr|
          holding_arr.each do |holding|
            holding.delete(:perm_location)
          end
        end
        hsh
      end

      # Note that the hash is the result of calling `to_hash`, not `to_marchash`
      def fields_from_marc_hash(hsh, codes)
        codes = [codes] if codes.kind_of? String
        hsh['fields'].select { |f| codes.include?(f.keys.first) }
      end

      def subfields_from_field(field, codes)
        codes = [codes] if codes.kind_of? String
        field['subfields'].select { |s| codes.include?(s.keys.first) }
      end

      def id_from_mfhd_hash(hsh)
        hsh['fields'].select { |f| f.has_key?('001') }.first['001']
      end

      def holdings_notes_from_mfhd_hash(hsh)
        notes = []
        f866_arr = fields_from_marc_hash(hsh, '866')
        f866_arr.each do |f|
          text_holdings = subfields_from_field(f['866'], 'a')
          public_note = subfields_from_field(f['866'], 'z')
          notes << text_holdings.first['a'] unless text_holdings.empty?
          notes << public_note.first['z'] unless public_note.empty?
        end
        notes
      end

      def callno_from_852(hsh_852)
        subfields = hsh_852.fetch('subfields', {})
        vals = subfields_from_field(hsh_852, ['h','i'])
        parts = []
        subfields_from_field(hsh_852, ['h','i']).each do |sf|
          parts << sf.values()
        end
        parts.flatten.join (' ')
      end

      def location_from_852(hsh_852)
        subfields = hsh_852.fetch('subfields', {})
        subfields_from_field(hsh_852, 'b').first['b']
      end

      # @param mfhd_id [Fixnum] A mfhd record id
      # @return [Array<Hash>] An Array of Hashes with three keys: :date, :li_status, :po_status.
      def get_orders(mfhd_id, conn=nil)
        statuses = []
        query = VoyagerHelpers::Queries.orders
        connection(conn) do |c|
          cursor = c.parse(query)
          cursor.bind_param(':mfhd_id', mfhd_id)
          cursor.exec()
          while row = cursor.fetch
            date = row[2].to_datetime unless date.nil?
            statuses << { po_status: row.shift,
                        li_status: row.shift,
                        date: date }
          end
          cursor.close()
        end
        statuses
      end

      def mfhd_is_suppressed?(mfhd_id, conn=nil)
        query = VoyagerHelpers::Queries.mfhd_suppressed
        connection(conn) do |c|
          exec_mfhd_is_suppressed?(query, mfhd_id, c)
        end
      end

      def exec_mfhd_is_suppressed?(query, mfhd_id, conn)
        suppressed = false
        connection(conn) do |c|
          cursor = c.parse(query)
          cursor.bind_param(':mfhd_id', mfhd_id)
          cursor.exec()
          suppressed = cursor.fetch == ['Y']
          cursor.close()
        end
        suppressed
      end

      def get_info_for_item(item_id, conn=nil, full=true)
        query = full == true ? VoyagerHelpers::Queries.full_item_info : VoyagerHelpers::Queries.brief_item_info
        connection(conn) do |c|
          exec_get_info_for_item(query, item_id, c, full)
        end
      end

      def exec_get_info_for_item(query, item_id, conn, full)
        info = {}
        row = []
        connection(conn) do |c|
          cursor = c.parse(query)
          cursor.bind_param(':item_id', item_id)
          cursor.exec()
          while r = cursor.fetch
            row = r
          end
          cursor.close()
        end
        info[:id] = row.shift
        info[:status] = row.shift
        info[:on_reserve] = row.shift
        info[:copy_number] = row.shift
        info[:item_sequence_number] = row.shift
        info[:temp_location] = row.shift
        if full == true
          info[:perm_location] = row.shift
          enum = row.shift
          info[:enum] = valid_ascii(enum)
          chron = row.shift
          info[:chron] = valid_ascii(chron)
          date = row.shift
          info[:status_date] = date.to_datetime unless date.nil?
          info[:barcode] = row.shift
        end
        if ['Charged', 'Renewed', 'Overdue'].include? info[:status]
          info[:due_date] = get_due_date_for_item(item_id, conn)
        end
        info
      end

      def get_due_date_for_item(item_id, conn)
        connection(conn) do |c|
          cursor = c.parse(VoyagerHelpers::Queries.item_due_date)
          cursor.bind_param(':item_id', item_id)
          cursor.exec()
          row = cursor.fetch
          cursor.close()
          due_date = row.shift if row
        end
      end

      def format_due_date(due_date, on_reserve)
        return if due_date.nil?
        unless due_date.to_datetime < DateTime.now-30
          if on_reserve == 'Y'
            due_date = due_date.strftime('%-m/%-d/%Y %l:%M%P')
          else
            due_date = due_date.strftime('%-m/%-d/%Y')
          end
        end
      end

      def valid_ascii(string)
        string.force_encoding("ascii").encode("UTF-8", {:invalid => :replace, :replace => ''}) unless string.nil?
      end

      def valid_codepoints(string)
        string.codepoints.map{|c| c.chr(Encoding::UTF_8)}.join unless string.nil?
      end

      def exec_get_info_for_patron(query, patron_id, conn)
        info = {}
        connection(conn) do |c|
          cursor = c.parse(query)
          cursor.bind_param(':id', patron_id)
          cursor.exec()
          row = cursor.fetch
          info[:netid] = row.shift
          f_name = row.shift
          info[:first_name] = valid_codepoints(f_name)
          l_name = row.shift
          info[:last_name] = valid_codepoints(l_name)
          info[:barcode] = row.shift
          info[:barcode_status] = row.shift
          info[:barcode_status_date] = row.shift
          info[:university_id] = row.shift
          patron_group = row.shift
          info[:patron_group] = patron_group == 3 ? 'staff' : patron_group
          info[:purge_date] = row.shift
          info[:expire_date] = row.shift
          info[:patron_id] = row.shift
          cursor.close()
          info[:active_email] = get_patron_email(info[:patron_id], c)
        end
        info
      end

      def get_patron_email(patron_id, conn)
        connection(conn) do |c|
          cursor = c.parse(VoyagerHelpers::Queries.patron_email)
          cursor.bind_param(':id', patron_id)
          cursor.exec()
          row = cursor.fetch
          cursor.close()
          email = row.shift if row
        end
      end

      def exec_get_patron_stat_codes(query, patron_id, conn)
        stat_codes = []
        connection(conn) do |c|
          cursor = c.parse(query)
          cursor.bind_param(':id', patron_id)
          cursor.exec()
          while row = cursor.fetch
            stat_codes << { stat_code: row.first }
          end
          cursor.close()
        end
        stat_codes
      end

      def accumulate_items_for_holding(mfhd_id, conn)
        items = []
        item_ids = get_item_ids_for_holding(mfhd_id, conn)
        item_ids.each do |item_id|
          items << get_info_for_item(item_id, conn)
        end
        items
      end

      def get_item_ids_for_holding(mfhd_id, conn)
        query = VoyagerHelpers::Queries.mfhd_item_ids
        connection(conn) do |c|
          exec_get_item_ids_for_holding(query, mfhd_id, c)
        end
      end

      def exec_get_item_ids_for_holding(query, mfhd_id, conn)
        item_ids = []
        connection(conn) do |c|
          cursor = c.parse(query)
          cursor.bind_param(':mfhd_id', mfhd_id)
          cursor.exec()
          while row = cursor.fetch
            item_ids << row.first
          end
          cursor.close()
        end
        item_ids.flatten
      end

      def bib_is_suppressed?(bib_id, conn=nil)
        suppressed = false
        query = VoyagerHelpers::Queries.bib_suppressed
        connection(conn) do |c|
          cursor = c.parse(query)
          cursor.bind_param(':bib_id', bib_id)
          cursor.exec()
          suppressed = cursor.fetch == ['Y']
          cursor.close()
        end
        suppressed
      end

      def get_bib_without_holdings(bib_id, conn=nil)
        segments = get_bib_segments(bib_id, conn)
        MARC::Reader.decode(segments.join(''), :external_encoding => "UTF-8", :invalid => :replace, :replace => '') unless segments.empty?
      end

      def get_bib_with_holdings(bib_id, conn=nil, opts={})
        bib = get_bib_without_holdings(bib_id, conn)
        unless bib.nil?
          holdings = get_holding_records(bib_id, conn)
          if opts.fetch(:holdings_in_bib, true)
            merge_holdings_into_bib(bib, holdings, conn)
          else
            [bib,holdings].flatten!
          end
        end
      end

      def merge_holdings_info(bib, holdings, conn=nil)
        record_hash = bib.to_hash
        record_hash['fields'].delete_if { |f| ['852', '866', '867', '868'].any? { |key| f.has_key?(key) } }
        unless holdings.empty?
          holdings.each do |holding|
            holding.to_hash['fields'].select { |h| ['852', '856', '866', '867', '868'].any? { |key| h.has_key?(key) } }.each do |h|
              key, _value = h.first # marc field hashes have only one key, which is the tag number
              h[key]['subfields'].unshift({"0"=>holding['001'].value})
              record_hash['fields'] << h
            end
          end
          catalog_date = get_catalog_date(bib['001'].value, holdings, conn)
          unless catalog_date.nil?
            record_hash['fields'] << {"959"=>{"ind1"=>" ", "ind2"=>" ", "subfields"=>[{"a"=>catalog_date.to_s}]}}
          end
        end
        record_hash
      end

      # Removes bib 852s and 86Xs, adds 852s, 856s, and 86Xs from holdings, adds 959 catalog date
      def merge_holdings_into_bib(bib, holdings, conn=nil)
        record_hash = merge_holdings_info(bib, holdings, conn)
        MARC::Record.new_from_hash(record_hash)
      end

      def get_catalog_date(bib_id, holdings, conn=nil)
        if electronic_resource?(holdings, conn)
          get_bib_create_date(bib_id, conn)
        else
          get_earliest_item_date(holdings, conn) # returns nil if no items
        end
      end

      def get_record_ids_from_barcode(barcode, conn=nil, recap=false)
        record_ids = []
        connection(conn) do |c|
          query = if recap
            VoyagerHelpers::Queries.recap_barcode_record_ids
          else
            VoyagerHelpers::Queries.barcode_record_ids
          end
          cursor = c.parse(query)
          cursor.bind_param(':barcode', barcode)
          cursor.exec()
          while row = cursor.fetch
            record_ids << row
          end
          cursor.close()
        end
        record_ids
      end

      def merge_holding_item_into_bib(bib, holding, item, recap=false, conn=nil)
        holdings = [holding]
        record_hash = merge_holdings_info(bib, holdings, conn)
        record_hash['fields'].delete_if { |f| ['876'].any? { |key| f.has_key?(key) } }
        holding_id = holding['001'].value
        holding_location = holding['852']['b']
        combined_call_no = ''
        if holding['852']['i']
          combined_call_no = "#{holding['852']['h']} #{holding['852']['i']}"
        end
        if recap && holding_location =~ /^rcp[a-z]{2}$/
          recap_item_hash = recap_item_info(holding_location)
          record_hash['fields'].delete_if { |f| ['852'].any? { |key| f.has_key?(key) } }
          holding.to_hash['fields'].select { |h| ['852'].any? { |key| h.has_key?(key) } }.each do |h|
            key, _value = h.first
            h[key]['subfields'].delete_if { |s| ['h', 'i'].any? { |key| s.has_key?(key) } }
            h[key]['subfields'].unshift({"0"=>holding_id})
            h[key]['subfields'].insert(2, {"h"=>combined_call_no})
            record_hash['fields'] << h
          end
          record_hash['fields'] << {"876"=>
            {"ind1"=>"0", "ind2"=>"0",
            "subfields"=>
              [
                {"0"=>holding_id.to_s},
                {"a"=>item[:id].to_s},
                {"h"=>recap_item_hash[:recap_use_restriction]},
                {"j"=>item[:status]},
                {"p"=>item[:barcode].to_s},
                {"t"=>item[:copy_number].to_s},
                {"x"=>recap_item_hash[:group_designation]},
                {"z"=>recap_item_hash[:customer_code]}
              ]
            }
          }
        else
          record_hash['fields'] << {"876"=>
            {"ind1"=>"0", "ind2"=>"0",
            "subfields"=>
              [
                {"0"=>holding_id.to_s},
                {"a"=>item[:id].to_s},
                {"j"=>item[:status]},
                {"p"=>item[:barcode].to_s},
                {"t"=>item[:copy_number].to_s}
              ]
            }
          }
        end
        MARC::Record.new_from_hash(record_hash)
      end

      def single_record_from_barcode (bib_id, mfhd_id, item_id, recap=false, conn=nil)
        merged_record = nil
        connection(conn) do |c|
          bib = get_bib_record(bib_id, c, {:holdings=>false})
          holding = get_holding_record(mfhd_id, c)
          item = get_item(item_id, c)
          merged_record = merge_holding_item_into_bib(bib, holding, item, recap, c)
        end
        merged_record
      end

      def recap_item_info(location)
        info_hash = {}
        customer_code = ''
        if location =~ /^rcpx[a-z]$/
          customer_code = 'PG'
        elsif location =~ /^rcp(?!x[a-z]).*$/
          customer_code = location.gsub(/^rcp([a-z]{2})/, '\1').upcase
        end
        info_hash[:customer_code] = customer_code
        recap_use_restriction = ''
        group_designation = ''
        case location
          when 'rcppa', 'rcpgp', 'rcpqk', 'rcppf'
            group_designation = 'Shared'
          when 'rcppj', 'rcppk', 'rcppl', 'rcppm', 'rcppn', 'rcppt'
            recap_use_restriction = 'In Library Use'
            group_designation = 'Private'
          when 'rcppb', 'rcpph', 'rcpps', 'rcppw', 'rcppz', 'rcpxc', 'rcpxg', 'rcpxm', 'rcpxn', 'rcpxp', 'rcpxr', 'rcpxx'
            recap_use_restriction = 'Supervised Use'
            group_designation = 'Private'
          when 'rcpjq', 'rcppe', 'rcppg', 'rcpph', 'rcppq', 'rcpqb', 'rcpql', 'rcpqv', 'rcpqx'
            group_designation = 'Private'
        end
        info_hash[:group_designation] = group_designation
        info_hash[:recap_use_restriction] = recap_use_restriction
        info_hash
      end

      def electronic_resource?(holdings, conn=nil)
        holdings.each do |mfhd|
          mfhd_hash = mfhd.to_hash
          field_852 = fields_from_marc_hash(mfhd_hash, '852').first['852']
          online = location_from_852(field_852).start_with?('elf')
          return true if online
        end
        false
      end

      def get_bib_create_date(bib_id, conn=nil)
        query = VoyagerHelpers::Queries.bib_create_date
        connection(conn) do |c|
          cursor = c.parse(query)
          cursor.bind_param(':bib_id', bib_id)
          cursor.exec()
          date = cursor.fetch.first
          cursor.close()
          date
        end
      end

      def get_item_create_date(item_id, conn=nil)
        query = VoyagerHelpers::Queries.item_create_date
        connection(conn) do |c|
          cursor = c.parse(query)
          cursor.bind_param(':item_id', item_id)
          cursor.exec()
          date = cursor.fetch.first
          cursor.close()
          date
        end
      end

      def get_earliest_item_date(holdings, conn=nil)
        item_ids = []
        holdings.each do |mfhd|
          mfhd_id = id_from_mfhd_hash(mfhd.to_hash)
          item_ids << get_item_ids_for_holding(mfhd_id, conn)
        end
        dates = []
        item_ids.flatten.min_by {|item_id| dates << get_item_create_date(item_id, conn)}
        dates.min
      end

      def get_bib_segments(bib_id, conn=nil)
        query = VoyagerHelpers::Queries.bib
        segments(query, bib_id, conn)
      end

      def get_mfhd_segments(mfhd_id, conn=nil)
        query = VoyagerHelpers::Queries.mfhd
        segments(query, mfhd_id, conn)
      end

      def segments(query, id, conn=nil)
        segments = []
        connection(conn) do |c|
          cursor = c.parse(query)
          cursor.bind_param(':id', id)
          cursor.exec()
          while row = cursor.fetch
            segments << row.first
          end
          cursor.close()
        end
        segments
      end

      def get_bib_mfhd_ids(bib_id, conn=nil)
        query = VoyagerHelpers::Queries.mfhd_ids
        connection(conn) do |c|
          exec_get_bib_mfhd_ids(query, bib_id, c)
        end
      end

      def exec_get_bib_mfhd_ids(query, bib_id, conn)
        ids = []
        connection(conn) do |c|
          cursor = c.parse(query)
          cursor.bind_param(':bib_id', bib_id)
          cursor.exec()
          while row = cursor.fetch
            ids << row.first
          end
          cursor.close()
        end
        ids
      end

      def determine_id_type(patron_id)
        if /^\d{14}$/.match(patron_id)
          'patron_barcode.patron_barcode'
        elsif /^\d{9}$/.match(patron_id)
          'patron.institution_id'
        else
          'patron.title'
        end
      end

    end # class << self
  end # class Liberator
end # module VoyagerHelpers
