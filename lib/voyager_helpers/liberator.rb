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
      # @option opts [Boolean] :holdings (true) (default) Include holdings?
      # @option opts [Boolean] :holdings_in_bib (true) (default) Copy 852 fields to the bib record?
      # @return [MARC::Record] If `holdings: false`, there are no holdings, or `holdings_in_bib: true`.
      # @return [Array<MARC::Record>] If `holdings: true`, `holdings_in_bib: false`, and there
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

      # @param timestamp [String] in format yyyy-mm-dd hh24:mi:ss.ffffff timezone_hourtimezone_minute (e.g., 2017-04-05 13:50:25.213245 -0400)
      # @return [Array]
      def get_updated_bibs(timestamp, conn=nil)
        bibs = []
        query = VoyagerHelpers::Queries.updated_bibs
        connection(conn) do |c|
          cursor = c.parse(query)
          cursor.bind_param(':last_diff_date', timestamp)
          cursor.exec
          while row = cursor.fetch
            bibs << row.first
          end
        end
        bibs
      end

      def get_all_bib_ids(conn = nil)
        bib_ids = []
        query = VoyagerHelpers::Queries.all_unsuppressed_bib_ids
        connection(conn) do |c|
          cursor = c.parse(query)
          cursor.exec
          while row = cursor.fetch
            bib_ids << row.first
          end
          cursor.close
        end
        bib_ids
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
      def get_holding_record(mfhd_id, conn=nil, recap=false)
        connection(conn) do |c|
          unless mfhd_is_suppressed?(mfhd_id, c) && recap == false
            segments = get_mfhd_segments(mfhd_id, c)
            MARC::Reader.decode(segments.join(''), :external_encoding => "UTF-8", :invalid => :replace, :replace => '') unless segments.empty?
          end
        end
      end

      # @param bib_id [Fixnum] A bib record id
      # @return [Array<MARC::Record>]
      def get_holding_records(bib_id, conn=nil, suppressed=false)
        records = []
        query = if suppressed
          VoyagerHelpers::Queries.mfhds_for_bib_supp
        else
          VoyagerHelpers::Queries.mfhds_for_bib
        end
        segments = []
        connection(conn) do |c|
          cursor = c.parse(query)
          cursor.bind_param(':bib_id', bib_id)
          cursor.exec
          while row = cursor.fetch
            segments << row.first
          end
          cursor.close
        end
        return segments if segments.empty?
        raw_marc = segments.join('')
        reader = MARC::Reader.new(StringIO.new(raw_marc, 'r'), external_encoding: 'UTF-8', invalid: :replace, replace: '')
        reader.each do |record|
          records << record
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

      def get_items_for_holding(mfhd_id, conn=nil)
        items = []
        query = VoyagerHelpers::Queries.all_mfhd_items
        rows = []
        connection(conn) do |c|
          cursor = c.parse(query)
          cursor.bind_param(':mfhd_id', mfhd_id)
          cursor.exec
          while row = cursor.fetch_hash
            rows << row
          end
          cursor.close
          items = group_item_info_rows(rows)
        end
        items
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
            mfhd_id = mfhd['001'].value.to_i
            holding_items = get_items_for_holding(mfhd_id, c)
            unless holding_items.empty?
              any_items = true
              data = { holding_id: mfhd_id }
              # Everyone seems quite sure that we don't repeat 852 per mfhd
              field_852 = mfhd['852']
              data[:perm_location] = field_852.nil? ? '' : field_852['b']
              data[:call_number] = callno_from_852(field_852)
              notes = holdings_notes_from_mfhd(mfhd)
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
              mfhd_id = mfhd['001'].value.to_i
              location = mfhd['852'].nil? ? '' : mfhd['852']['b']
              holding_item_ids = get_item_ids_for_holding(mfhd_id, c)
              availability[bib_id][mfhd_id] = {} # holding record availability hash
              availability[bib_id][mfhd_id][:more_items] = holding_item_ids.count > 1
              availability[bib_id][mfhd_id][:location] = location
              availability[bib_id][mfhd_id][:status] = if holding_item_ids.empty?
                order_status = get_order_status(mfhd_id, c)
                if order_status
                  order_status
                elsif location =~ /^elf/
                  'Online'
                else
                  'On Shelf'
                end
              else
                byebug
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
          item_hash[:item_type] = item[:item_type]
          item_hash[:pickup_location_id] = item[:pickup_location_id]
          item_hash[:pickup_location_code] = item[:pickup_location_code]
          item_hash[:patron_group_charged] = item[:patron_group_charged]
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
      def get_current_issues(mfhd_id, conn = nil)
        issues = []
        connection(conn) do |c|
          cursor = c.parse(VoyagerHelpers::Queries.current_periodicals)
          cursor.bind_param(':mfhd_id', mfhd_id)
          cursor.exec
          while enum = cursor.fetch
            issues << enum.first
          end
          cursor.close
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

      # param file_stub [String] Filename pattern
      # param slice_size [Int] How many records per file
      # param opts [Hash] Supply holdings => false if a dump without
      # merged holdings is wanted
      # Dumps all bib records with merged holdings to MARC21
      def full_bib_dump(file_stub, slice_size, opts={})
        connection do |c|
          all_bibs = get_all_bib_ids(c)
          file_num = 1
          all_bibs.each_slice(slice_size) do |bib_slice|
            file_name = "#{file_stub}-#{file_num}.mrc"
            dump_bibs_to_file(bib_slice, file_name, c, opts)
            file_num += 1
          end
        end
      end

      # param ids [Array] Array of bib IDs
      # param opts [Hash] Supply holdings => false if records without
      # merged holdings are wanted
      # It is possible that some MARC records will be oversized, due to adding in fields;
      # MARC::ForgivingReader should be used for methods that read in these files
      def dump_bibs_to_file(ids, file_name, conn=nil, opts={})
        writer = MARC::Writer.new(file_name)
        writer.allow_oversized = true
        connection(conn) do |c|
          ids.each_slice(1000) do |bib_ids|
            bibs = get_bib_coll(bib_ids, c)
            if opts.fetch(:holdings, true)
              all_mfhds = get_mfhds_for_bib_coll(bib_ids, c)
              bib_create_dates = {}
              earliest_item_dates = {}
              if opts.fetch(:cat_date, true)
                electronic_bibs = Set.new
                all_mfhds.each do |bib_id, mfhd|
                  reader = MARC::Reader.new(StringIO.new(mfhd, 'r'), external_encoding: 'UTF-8', invalid: :replace, replace: '')
                  reader.each do |holding|
                    electronic_bibs << bib_id if holding['852'] && holding['852']['b'] =~ /^elf/
                  end
                end
                bib_create_dates = get_bulkbib_create_dates(electronic_bibs.to_a, c) unless electronic_bibs.empty?
                earliest_item_dates = get_bulkbib_earliest_item_dates(bib_ids, c) unless electronic_bibs.length == all_mfhds.length
              end
              bibs.each do |bib|
                next unless bib['001']
                bib.fields.delete_if { |f| ['852', '866', '867', '868'].include? f.tag }
                bib_id = bib['001'].value.to_i
                mfhds = all_mfhds[bib_id]
                unless mfhds.nil?
                  mfhd_reader = MARC::Reader.new(StringIO.new(mfhds, 'r'), external_encoding: 'UTF-8', invalid: :replace, replace: '')
                  mfhd_reader.each do |holding|
                    holding.fields.each_by_tag(['852', '856', '866', '867', '868']) do |field|
                      field.subfields.unshift(MARC::Subfield.new('0', holding['001'].value))
                      bib.append(field)
                    end
                  end
                  if opts.fetch(:cat_date, true)
                    h_fields = bib.fields('852')
                    unless h_fields.empty?
                      cat_date = bib_create_dates[bib_id] || earliest_item_dates[bib_id]
                      bib.append(MARC::DataField.new('959', ' ', ' ', ['a', cat_date.to_s])) if cat_date
                    end
                  end
                end
                writer.write(bib)
              end
            else
              bibs.each do |bib|
                writer.write(bib)
              end
            end
          end
        end
        writer.close
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
      def get_order_status(mfhd_id, conn=nil)
        status = nil
        ledger_id = get_ledger(conn)
        orders = get_orders(mfhd_id, ledger_id, conn)
        unless orders.empty?
          latest_order = orders.max { |a, b| a[:date] <=> b[:date] }
          po_status, li_status = latest_order[:po_status], latest_order[:li_status]
          if on_order?(po_status, li_status)
            issues = get_current_issues(mfhd_id, conn)
            status = if issues.size > 0
              nil
            elsif li_status == li_rec_complete
              'Order Received'
            elsif li_status == li_pending
              'Pending Order'
            else
              'On-Order'
            end
            status << " #{latest_order[:date].strftime('%m-%d-%Y')}" unless status.nil? || latest_order[:date].nil?
          end
        end
        status
      rescue ArgumentError => error
        Rails.logger.error "Failed to parse the Voyager query results #{orders.join(',')} : #{error}"
        return
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
        connection do |c|
          items = updated_recap_items(date, c)
          items.each do |item|
            item_statuses = get_item_statuses(item, c)
            unless item_statuses.include?('In Process')
              item_barcode = get_barcode_from_item(item, c)
              barcodes << item_barcode
            end
          end
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

      def subfields_from_field(field, codes)
        codes = [codes] if codes.kind_of? String
        field.subfields.select { |s| codes.include?(s.code) }
      end

      def holdings_notes_from_mfhd(mfhd)
        notes = []
        f866_arr = mfhd.fields('866')
        f866_arr.each do |f|
          text_holdings = f['a']
          public_notes = f.subfields.select { |subf| subf.code == 'z' }
          notes << text_holdings unless text_holdings.nil?
          public_notes.each { |note| notes << note }
        end
        notes
      end

      def callno_from_852(field_852)
        call_no = field_852['h']
        return call_no if call_no.nil?
        call_no << ' ' + field_852['i'] if field_852['i']
        call_no.gsub!(/^[[:blank:]]+(.*)$/, '\1')
        call_no
      end

      # @return [Fixnum] A ledger ID
      def get_ledger(conn=nil)
        query = VoyagerHelpers::Queries.ledger
        date = Date.today
        year = date.year
        month = date.month
        year += 1 if month > 6
        year = year.to_s
        connection(conn) do |c|
          cursor = c.parse(query)
          cursor.bind_param(':year', year)
          cursor.exec
          row = cursor.fetch
          row.first
        end
      end

      # @param mfhd_id [Fixnum] A mfhd record id
      # @param ledger_id [Fixnum] The current ledger ID
      # @return [Array<Hash>] An Array of Hashes with three keys: :date, :li_status, :po_status.
      def get_orders(mfhd_id, ledger_id, conn=nil)
        statuses = []
        query = VoyagerHelpers::Queries.orders
        connection(conn) do |c|
          cursor = c.parse(query)
          cursor.bind_param(':mfhd_id', mfhd_id)
          cursor.bind_param(':ledger_id', ledger_id)
          cursor.exec
          while row = cursor.fetch
            date = row[2] ? row[2].to_datetime : row[2]
            statuses << { po_status: row.shift,
                        li_status: row.shift,
                        date: date }
          end
          cursor.close
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
          row = cursor.fetch
          cursor.close()
        end
        info[:id] = row.shift
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
          info[:barcode] = row.shift
        end
        info[:status] = get_item_statuses(item_id, conn)
        unless (info[:status] & ['Charged', 'Renewed', 'Overdue']).empty?
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

      def get_item_statuses(item_id, conn=nil)
        query = VoyagerHelpers::Queries.item_statuses
        statuses = []
        connection do |c|
          cursor = c.parse(query)
          cursor.bind_param(':item_id', item_id)
          cursor.exec()
          while row = cursor.fetch
            statuses << row.first
          end
        end
        statuses
      end

      def get_barcode_from_item(item_id, conn=nil)
        barcode = ''
        query = VoyagerHelpers::Queries.barcode_from_item
        connection do |c|
          cursor = c.parse(query)
          cursor.bind_param(':item_id', item_id)
          cursor.exec()
          barcode = cursor.fetch.first
          cursor.close()
        end
        barcode
      end

      def updated_recap_items(date, conn=nil)
        items = []
        connection(conn) do |c|
          query = VoyagerHelpers::Queries.recap_update_bib_items
          cursor = c.parse(query)
          cursor.bind_param(':last_diff_date', date)
          cursor.exec()
          while row = cursor.fetch
            items << row.first
          end
          cursor.close()
          query = VoyagerHelpers::Queries.recap_update_holding_items
          cursor = c.parse(query)
          cursor.bind_param(':last_diff_date', date)
          cursor.exec()
          while row = cursor.fetch
            items << row.first
          end
          cursor.close()
          query = VoyagerHelpers::Queries.recap_update_item_items
          cursor = c.parse(query)
          cursor.bind_param(':last_diff_date', date)
          cursor.exec()
          while row = cursor.fetch
            items << row.first
          end
          cursor.close()
        end
        items
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

      def group_item_info_rows(rows)
        final_items = []
        grouped_items = rows.group_by { |row| row['ITEM_ID'] }
        grouped_items.each do |_item_id, items|
          statuses = []
          first_item = items.first
          info = {}
          info[:id] = first_item['ITEM_ID']
          info[:on_reserve] = first_item['ON_RESERVE']
          info[:copy_number] = first_item['COPY_NUMBER']
          info[:item_sequence_number] = first_item['ITEM_SEQUENCE_NUMBER']
          info[:temp_location] = first_item['TEMP_LOC']
          info[:perm_location] = first_item['LOCATION_CODE']
          info[:circ_group_id] = first_item['CIRC_GROUP_ID']
          info[:circ_group_id] ||= 1
          pickup_loc = pickup_location_for_circ_group_id[info[:circ_group_id]]
          pickup_loc ||= pickup_location_for_circ_group_id[1] # default to Firestone
          info[:pickup_location_code] = pickup_loc[:code]
          info[:pickup_location_id] = pickup_loc[:id]
          enum = first_item['ITEM_ENUM']
          info[:enum] = valid_ascii(enum)
          chron = first_item['CHRON']
          info[:chron] = valid_ascii(chron)
          info[:barcode] = first_item['ITEM_BARCODE']
          info[:item_type] = first_item['ITEM_TYPE_CODE']
          info[:due_date] = first_item['CURRENT_DUE_DATE']
          info[:patron_group_charged] = first_item['PATRON_GROUP_CODE']
          items.each do |item|
            statuses << item['ITEM_STATUS_DESC']
          end
          info[:status] = statuses
          final_items << info
        end
        final_items
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

      def get_bib_coll(bib_ids, conn=nil)
        segments = get_bib_coll_segments(bib_ids, conn)
        return nil if segments.empty?
        raw_marc = segments.join('')
        MARC::Reader.new(StringIO.new(raw_marc, 'r'), external_encoding: 'UTF-8', invalid: :replace, replace: '')
      end

      def get_bib_with_holdings(bib_id, conn=nil, opts={})
        bib = get_bib_without_holdings(bib_id, conn)
        unless bib.nil?
          holdings = get_holding_records(bib_id, conn)
          if opts.fetch(:holdings_in_bib, true)
            merge_holdings_info(bib, holdings, conn)
          else
            [bib,holdings].flatten!
          end
        end
      end

      def get_mfhds_for_bib_coll(bib_ids, conn = nil)
        query = VoyagerHelpers::Queries.mfhds_for_bibs(bib_ids)
        segments = []
        connection(conn) do |c|
          c.exec(query, *bib_ids) do |row|
            segments << { bib_id: row[0], mfhd_data: row[1] }
          end
        end
        mfhds_with_keys = segments.group_by { |segment| segment[:bib_id] }
        mfhd_collection ={}
        mfhds_with_keys.each do |key, value|
          bib_id = key
          mfhd_segments = value.map { |hash| hash[:mfhd_data] }
          mfhd_collection[bib_id] = mfhd_segments.join('')
        end
        mfhd_collection
      end

      # Removes bib 852s and 86Xs, adds 852s, 856s, and 86Xs from holdings, adds 959 catalog date
      def merge_holdings_info(bib, holdings, conn=nil)
        merged_bib = bib
        merged_bib.fields.delete_if { |f| ['852', '866', '867', '868'].include? f.tag }
        unless holdings.empty?
          holdings.each do |holding|
            holding.fields.each_by_tag(['852', '856', '866', '867', '868']) do |field|
              field.subfields.unshift(MARC::Subfield.new('0', holding['001'].value))
              merged_bib.append(field)
            end
          end
          catalog_date = get_catalog_date(bib['001'].value, holdings, conn)
          unless catalog_date.nil?
            merged_bib.append(MARC::DataField.new('959', ' ', ' ', ['a', catalog_date.to_s]))
          end
        end
        merged_bib
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
        merged_bib = merge_holdings_info(bib, holdings, conn)
        merged_bib.fields.delete_if { |f| f.tag == '876' }
        holding_id = holding['001'].value
        holding_location = holding['852']['b']
        item_enum_chron = nil
        if item[:enum]
          item_enum_chron = item[:enum]
          unless item[:chron].nil?
            item_enum_chron << " (#{item[:chron]})"
          end
        elsif item[:chron]
          item_enum_chron = item[:chron]
        end
        if recap && holding_location =~ /^rcp[a-z]{2}$/
          call_no = callno_from_852(holding['852'])
          recap_item_hash = recap_item_info(holding_location)
          merged_bib.fields.delete_if { |f| f.tag == '852' }
          f852 = holding['852']
          f852.subfields.delete_if { |s| ['h', 'i'].include? s.code }
          f852.subfields.unshift(MARC::Subfield.new('0', holding['001'].value))
          f852.append(MARC::Subfield.new('h', call_no))
          merged_bib.append(f852)
          merged_bib.append(MARC::DataField.new('876', '0', '0',
            MARC::Subfield.new('0', holding_id.to_s),
            MARC::Subfield.new('3', item_enum_chron),
            MARC::Subfield.new('a', item[:id].to_s),
            MARC::Subfield.new('h', recap_item_hash[:recap_use_restriction]),
            MARC::Subfield.new('j', item[:status].join(', ')),
            MARC::Subfield.new('p', item[:barcode].to_s),
            MARC::Subfield.new('t', item[:copy_number].to_s),
            MARC::Subfield.new('x', recap_item_hash[:group_designation]),
            MARC::Subfield.new('z', recap_item_hash[:customer_code]))
          )
        else
          merged_bib.append(MARC::DataField.new('876', '0', '0',
            MARC::Subfield.new('0', holding_id.to_s),
            MARC::Subfield.new('3', item_enum_chron),
            MARC::Subfield.new('a', item[:id].to_s),
            MARC::Subfield.new('j', item[:status].join(', ')),
            MARC::Subfield.new('p', item[:barcode].to_s),
            MARC::Subfield.new('t', item[:copy_number].to_s))
          )
        end
        merged_bib
      end

      def single_record_from_barcode (bib_id, mfhd_id, item_id, recap=false, conn=nil)
        merged_record = nil
        connection(conn) do |c|
          bib = get_bib_without_holdings(bib_id, c)
          holding = get_holding_record(mfhd_id, c, recap)
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
          when 'rcppb', 'rcpph', 'rcpps', 'rcppw', 'rcppz', 'rcpxc', 'rcpxg', 'rcpxm', 'rcpxn', 'rcpxp', 'rcpxr', 'rcpxw', 'rcpxx'
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
          next unless mfhd['852']
          return true if mfhd['852']['b'] =~ /^elf/
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

      def get_bulkbib_create_dates(bib_ids, conn=nil)
        create_dates = {}
        query = VoyagerHelpers::Queries.bulkbib_create_date(bib_ids)
        connection(conn) do |c|
          c.exec(query, *bib_ids) do |row|
            bib_id = row.shift
            date = row.shift
            create_dates[bib_id] = date
          end
        end
        create_dates
      end

      def get_bulkbib_earliest_item_dates(bib_ids, conn=nil)
        item_dates = {}
        query = VoyagerHelpers::Queries.bulkbib_earliest_item_date(bib_ids)
        connection(conn) do |c|
          c.exec(query, *bib_ids) do |row|
            bib_id = row.shift.to_i
            date = row.shift
            item_dates[bib_id] = date
          end
        end
        item_dates
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
          mfhd_id = mfhd['001'].value
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

      def get_bib_coll_segments(bib_ids, conn = nil)
        query = VoyagerHelpers::Queries.bulk_bib(bib_ids)
        segments = []
        connection(conn) do |c|
          c.exec(query, *bib_ids) do |row|
            segments << row.first
          end
        end
        segments
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

      def determine_id_type(patron_id)
        if /^\d{14}$/.match(patron_id)
          'patron_barcode.patron_barcode'
        elsif /^\d{9}$/.match(patron_id)
          'patron.institution_id'
        else
          'patron.title'
        end
      end

      def pickup_location_for_circ_group_id
        {
          1 => { code: 'fcirc', id: 299 }, # Firestone
          5 => { code: 'uescirc', id: 356 }, # Architecture
          6 => { code: 'muscirc', id: 309 }, # Music
          7 => { code: 'sacirc', id: 321 }, # Marquand
          10 => { code: 'anxacirc', id: 293 }, # Annex A
          13 => { code: 'piaprcirc', id: 333 }, # Stokes
          14 => { code: 'stcirc', id: 345 }, # Engineering
          15 => { code: 'gestcirc', id: 303 }, # East Asian
          16 => { code: 'pplcirc', id: 312 }, # PPPL
          17 => { code: 'muddcirc', id: 306 }, # Mudd
          18 => { code: 'rbcirc', id: 315 }, # Rare Books
          21 => { code: 'fcirc', id: 299 }, # Video Library (Firestone pickup)
          22 => { code: 'fcirc', id: 299 }, # ReCAP (Firestone pickup)
          24 => { code: 'scicirc', id: 489 } # Lewis
        }
      end
    end # class << self
  end # class Liberator
end # module VoyagerHelpers
