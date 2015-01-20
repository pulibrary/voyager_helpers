module VoyagerHelpers
  module Queries
    class << self

      def bib_suppressed(bib_id)
        %Q(
        SELECT suppress_in_opac FROM bib_master
        WHERE bib_id=#{bib_id}
        )
      end

      def all_locations
        %Q(
        SELECT location_id, location_code, location_display_name,
        suppress_in_opac
        FROM location
        ORDER BY location_id
        )
      end

      def item_info(item_id)
        %Q(
        SELECT 
          item.item_id,
          item.copy_number,
          item.item_sequence_number,
          item.on_reserve,
          perm_loc.location_code,
          temp_loc.location_code,
          item_status_type.item_status_desc,
          item_status.item_status_date,
          item_barcode.item_barcode
        FROM item
          INNER JOIN location perm_loc 
            ON perm_loc.location_id = item.perm_location
          LEFT JOIN location temp_loc 
            ON temp_loc.location_id = item.temp_location
          INNER JOIN item_status
            ON item_status.item_id = item.item_id
          INNER JOIN item_status_type
            ON item_status_type.item_status_type = item_status.item_status
          LEFT JOIN item_barcode
            ON item_barcode.item_id = item.item_id
        WHERE item.item_id=#{item_id}
        )
      end

      def approved_orders(bib_id)
        # what could go wrong?
        line_item_status_approved=8
        po_status_approved=1
        %Q(
        SELECT bib_text.bib_id, 
          purchase_order.po_status,
          line_item_copy_status.line_item_status,
          line_item_copy_status.status_date
        FROM ((purchase_order
        INNER JOIN line_item ON purchase_order.po_id = line_item.po_id)
        INNER JOIN bib_text ON line_item.bib_id = bib_text.bib_id)
        INNER JOIN line_item_copy_status ON line_item.line_item_id = line_item_copy_status.line_item_id
        WHERE (((bib_text.bib_id)=#{bib_id})
        AND ((purchase_order.po_status)=#{po_status_approved}))
        OR (((bib_text.bib_id)=#{bib_id})
        AND ((line_item_copy_status.line_item_status)=#{line_item_status_approved}))
        )
      end

      def statuses
        %Q(
        SELECT item_status_type, item_status_desc
        FROM item_status_type
        )
      end

      def bib(bib_id)
        %Q(
        SELECT record_segment 
        FROM bib_data
        WHERE bib_id=#{bib_id}
        ORDER BY seqnum
        )
      end

      def all_unsupressed_bib_ids
        %Q(
        SELECT bib_id 
        FROM bib_master
        WHERE bib_master.suppress_in_opac='N'
        )
      end

      def mfhd(mfhd_id)
        %Q(
        SELECT record_segment FROM mfhd_data
        WHERE mfhd_id=#{mfhd_id}
        ORDER BY seqnum
        )
      end

      def mfhd_suppressed(mfhd_id)
        %Q(
        SELECT * 
        FROM ( 
          SELECT suppress_in_opac 
          FROM mfhd_history
          WHERE mfhd_id=#{mfhd_id}
          ORDER BY action_date DESC
        ) 
        WHERE ROWNUM=1
        )
      end

      def mfhd_ids(bib_id)
        %Q(
        SELECT mfhd_id 
        FROM bib_mfhd
        WHERE bib_id=#{bib_id}
        )
      end

      def mfhd_item_ids(mfhd_id)
        %Q(
        SELECT item_id FROM mfhd_item
        WHERE mfhd_id=#{mfhd_id}
        )
      end

    end # class << self
  end # module Queries
end # module VoyagerHelpers








