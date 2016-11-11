module VoyagerHelpers
  module Queries
    class << self

      def recap_locations
        %w(
          378
          419
          420
          421
          422
          423
          424
          425
          426
          427
          428
          429
          436
          438
          446
          448
          454
          459
          461
          462
          464
          465
          466
          491
          493
          494
          495
          496
          497
          498
          503
          504
          515
        )
      end

      def barcode_record_ids(barcodes)
        barcodes = OCI8::in_cond(:barcodes, barcodes)
        %Q(
          SELECT
            bib_item.bib_id,
            mfhd_item.mfhd_id,
            item_barcode.item_id
          FROM item_barcode
            JOIN mfhd_item
              ON item_barcode.item_id = mfhd_item.item_id
            JOIN bib_item
              ON item_barcode.item_id = bib_item.item_id
            JOIN mfhd_master
              ON mfhd_item.mfhd_id = mfhd_master.mfhd_id
            JOIN bib_master
              ON bib_item.bib_id = bib_master.bib_id
          WHERE
            item_barcode.item_barcode IN (#{barcodes.names})
            AND bib_master.suppress_in_opac = 'N'
            AND mfhd_master.suppress_in_opac = 'N'
            AND item_barcode.barcode_status = 1
        )
      end

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

      def full_item_info(item_id)
        %Q(
        SELECT
          item.item_id,
          item_status_type.item_status_desc,
          item.on_reserve,
          item.copy_number,
          item.item_sequence_number,
          temp_loc.location_code,
          perm_loc.location_code,
          mfhd_item.item_enum,
          mfhd_item.chron,
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
          INNER JOIN mfhd_item
            ON mfhd_item.item_id = item.item_id
          LEFT JOIN item_barcode
            ON item_barcode.item_id = item.item_id
        WHERE item.item_id=#{item_id} AND
          item_status.item_status NOT IN ('5', '6', '16', '19', '20', '21', '23', '24') AND
          (item_barcode.barcode_status = 1 OR item_barcode.barcode_status IS NULL)
        )
      end

      def brief_item_info(item_id)
        %Q(
        SELECT
          item.item_id,
          item_status_type.item_status_desc,
          item.on_reserve,
          item.copy_number,
          item.item_sequence_number,
          temp_loc.location_code
        FROM item
          LEFT JOIN location temp_loc
            ON temp_loc.location_id = item.temp_location
          INNER JOIN item_status
            ON item_status.item_id = item.item_id
          INNER JOIN item_status_type
            ON item_status_type.item_status_type = item_status.item_status
        WHERE item.item_id=#{item_id} AND
          item_status.item_status NOT IN ('5', '6', '16', '19', '20', '21', '23', '24')
        )
      end

      def item_create_date(item_id)
        %Q(
        SELECT
          create_date
        FROM item
        WHERE item_id=#{item_id}
        )
      end

      def orders(mfhd_id)
        mfhd_id = OCI8::in_cond(:mfhd_id, mfhd_id)
        %Q(
        SELECT
          purchase_order.po_status,
          line_item_copy_status.line_item_status,
          line_item_copy_status.status_date
        FROM line_item_copy_status
          JOIN line_item
            ON line_item_copy_status.line_item_id = line_item.line_item_id
          JOIN purchase_order
            ON line_item.po_id = purchase_order.po_id
        WHERE
          line_item_copy_status.mfhd_id IN (#{mfhd_id.names})
        ORDER BY
          line_item_copy_status.status_date DESC
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

      def bib_id_for_holding_id(mfhd_id)
        %Q(
        SELECT
          bib_master.bib_id,
          bib_master.create_date,
          bib_master.update_date
        FROM bib_master
          INNER JOIN bib_mfhd
            ON bib_mfhd.mfhd_id=#{mfhd_id}
        WHERE bib_master.bib_id = bib_mfhd.bib_id
        )
      end

      def all_unsupressed_bib_ids
        %Q(
        SELECT
          bib_id,
          create_date,
          update_date
        FROM bib_master
        WHERE bib_master.suppress_in_opac='N'
        )
      end

      def bib_create_date(bib_id)
        %Q(
        SELECT
          create_date
        FROM bib_master
        WHERE bib_master.bib_id=#{bib_id}
        )
      end

      def bib_update_date(bib_id)
        %Q(
        SELECT
          update_date
        FROM bib_master
        WHERE bib_master.bib_id=#{bib_id}
        )
      end

      def mfhd_update_date(mfhd_id)
        %Q(
        SELECT
          update_date
        FROM mfhd_master
        WHERE mfhd_master.mfhd_id=#{mfhd_id}
        )
      end

      def all_unsupressed_mfhd_ids
        %Q(
        SELECT
          mfhd_master.mfhd_id,
          mfhd_master.create_date,
          mfhd_master.update_date
        FROM mfhd_master
          INNER JOIN location
            ON mfhd_master.location_id = location.location_id
        WHERE mfhd_master.suppress_in_opac='N'
          AND location.suppress_in_opac='N'
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
          SELECT suppress_in_opac
          FROM mfhd_master
          WHERE mfhd_id=#{mfhd_id}
        )
      end

      def record_ids_for_barcode
        %Q(
          SELECT 
            bib_master.bib_id,
            mfhd_master.mfhd_id,
            item_barcode.item_id
          FROM item_barcode 
            JOIN bib_item 
              ON item_barcode.item_id = bib_item.item_id
            JOIN bib_master
              ON bib_item.bib_id = bib_master.bib_id
            JOIN bib_mfhd
              ON bib_master.bib_id = bib_mfhd.bib_id
            JOIN mfhd_master
              ON bib_mfhd.mfhd_id = mfhd_master.mfhd_id
          WHERE
            bib_master.suppress_in_opac = 'N' AND
            mfhd_master.suppress_in_opac = 'N' AND
            item_barcode.barcode_status = 1 AND
            item_barcode.item_barcode = :barcode
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

      def patron_info(id, id_field)
        %Q(
          SELECT
            patron.title,
            patron.first_name,
            patron.last_name,
            patron_barcode.patron_barcode,
            patron_barcode.barcode_status,
            patron_barcode.barcode_status_date,
            patron.institution_id,
            patron_barcode.patron_group_id,
            patron.purge_date,
            patron.expire_date,
            patron.patron_id
          FROM patron, patron_barcode
          WHERE
            #{id_field}='#{id}'
            AND patron.patron_id=patron_barcode.patron_id
            AND patron_barcode.barcode_status=1
          )
      end

      def patron_stat_codes(id, id_field)
        %Q(
          SELECT
            patron_stat_code.patron_stat_desc
          FROM patron_stat_code
            JOIN patron_stats
              ON patron_stat_code.patron_stat_id = patron_stats.patron_stat_id
            JOIN patron
              ON patron_stats.patron_id = patron.patron_id
            JOIN patron_barcode
              ON patron.patron_id = patron_barcode.patron_id
          WHERE
            #{id_field}='#{id}'
            AND patron_barcode.barcode_status = 1
        )
      end

      def active_courses
        %Q(
          SELECT
            reserve_list.reserve_list_id,
            department.department_name,
            department.department_code,
            course.course_name,
            course.course_number,
            reserve_list_courses.section_id,
            instructor.first_name,
            instructor.last_name
          FROM reserve_list_courses 
            JOIN department
              ON reserve_list_courses.department_id = department.department_id
            JOIN instructor
              ON reserve_list_courses.instructor_id = instructor.instructor_id
            JOIN course
              ON reserve_list_courses.course_id = course.course_id
            JOIN reserve_list
              ON reserve_list.reserve_list_id = reserve_list_courses.reserve_list_id
            JOIN reserve_list_items
              ON reserve_list.reserve_list_id = reserve_list_items.reserve_list_id
          WHERE 
            reserve_list.expire_date >= sysdate
            AND reserve_list.effect_date <= sysdate
          GROUP BY
            reserve_list.reserve_list_id,
            department.department_name,
            department.department_code,
            course.course_name,
            course.course_number,
            reserve_list_courses.section_id,
            instructor.first_name,
            instructor.last_name
            )
      end

      def course_bibs(ids)
        ids = OCI8::in_cond(:id, ids)
        %Q(
          SELECT
            reserve_list.reserve_list_id,
            bib_item.bib_id
          FROM ((reserve_list join
               reserve_list_items on reserve_list.reserve_list_id = reserve_list_items.reserve_list_id) join
               bib_item on reserve_list_items.item_id = bib_item.item_id)
          WHERE reserve_list.reserve_list_id IN (#{ids.names})
          GROUP BY reserve_list.reserve_list_id,
                    bib_item.bib_id
        )
      end

      def current_periodicals
        %Q(
        SELECT
          serial_issues.enumchron
        FROM subscription
          JOIN component
            ON subscription.subscription_id = component.subscription_id
          JOIN issues_received
            ON component.component_id = issues_received.component_id
          JOIN serial_issues
            ON issues_received.component_id = serial_issues.component_id
              AND issues_received.issue_id = serial_issues.issue_id
          JOIN line_item_copy_status
            ON subscription.line_item_id = line_item_copy_status.line_item_id
          JOIN line_item
            ON line_item_copy_status.line_item_id = line_item.line_item_id
        WHERE
          line_item_copy_status.mfhd_id = :mfhd_id
          AND serial_issues.received = 1
          AND issues_received.opac_suppressed = 1
        ORDER BY serial_issues.component_id DESC, serial_issues.issue_id DESC
        )
      end
    end # class << self
  end # module Queries
end # module VoyagerHelpers
