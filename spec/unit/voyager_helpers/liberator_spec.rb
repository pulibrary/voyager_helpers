require 'spec_helper'
require 'json'
require 'time'

def json_stub(filename)
  record = fixture("/#{filename}.json")
  record = JSON.parse(record.read)
  record = MARC::Record.new_from_hash(record)
end

describe VoyagerHelpers::Liberator do
  let(:placeholder_id) { 12345 }

  describe '#get_order_status' do
    let(:date) { Date.parse("2015-12-14T15:34:00.000-05:00").to_datetime }
    let(:newer_date) { Date.parse("2016-12-14T15:34:00.000-05:00").to_datetime }
    let(:no_order_found) { {} }
    let(:pre_order) { [{
                        date: nil,
                        li_status: 0,
                        po_status: 0
                        }] }
    let(:approved_order) { [{
                        date: date,
                        li_status: 8,
                        po_status: 1
                        }] }
    let(:partially_rec_order) { [{
                        date: newer_date,
                        li_status: 8,
                        po_status: 3
                        }] }
    let(:received_order) { [{
                        date: date,
                        li_status: 1,
                        po_status: 4
                        }] }
    let(:complete_order) { [{
                        date: date,
                        li_status: 9,
                        po_status: 5
                        }] }
    let(:canceled_order) { {
                        date: date,
                        li_status: 7,
                        po_status: 6
                        } }
    let(:two_orders) { [canceled_order, partially_rec_order.first] }


    it 'returns nil when no order found for bib' do
      allow(described_class).to receive(:get_orders).and_return(no_order_found)
      expect(described_class.get_order_status(placeholder_id)).to eq nil
    end
    it 'returns Pending Order for pending orders, date not included if nil' do
      allow(described_class).to receive(:get_orders).and_return(pre_order)
      expect(described_class.get_order_status(placeholder_id)).to eq "Pending Order"
    end
    it 'returns On-Order for approved order' do
      allow(described_class).to receive(:get_orders).and_return(approved_order)
      expect(described_class.get_order_status(placeholder_id)).to include('On-Order')
    end
    it 'returns On-Order for partially received order' do
      allow(described_class).to receive(:get_orders).and_return(partially_rec_order)
      expect(described_class.get_order_status(placeholder_id)).to include('On-Order')
    end
    it 'returns Order Received for fully received order' do
      allow(described_class).to receive(:get_orders).and_return(received_order)
      expect(described_class.get_order_status(placeholder_id)).to include('Order Received')
    end
    it "includes status date with order response" do
      allow(described_class).to receive(:get_orders).and_return(approved_order)
      expect(described_class.get_order_status(placeholder_id)).to include(date.strftime('%m-%d-%Y'))
    end
    it "it returns nil when order is complete" do
      allow(described_class).to receive(:get_orders).and_return(complete_order)
      expect(described_class.get_order_status(placeholder_id)).to eq nil
    end
    it 'returns order status of newer order when multiple orders' do
      allow(described_class).to receive(:get_orders).and_return(two_orders)
      expect(described_class.get_order_status(placeholder_id)).to include('On-Order')
    end
  end

  describe '#get_full_mfhd_availability' do
    let(:item_id) { 36736 }
    let(:item_2_id) { 36737 }
    let(:item_3_id) { 36738 }
    let(:item_barcode) { '32101005535917' }
    let(:not_charged) { 'Not Charged' }
    let(:charged) { 'Charged' }
    let(:single_volume_2_copy) { [{
                                id: item_id,
                                status: not_charged,
                                on_reserve: 'N',
                                temp_location: nil,
                                perm_location: 'f',
                                enum: nil,
                                chron: nil,
                                copy_number: 2,
                                item_sequence_number: 1,
                                status_date: '2014-05-27T06:00:19.000-05:00',
                                barcode: item_barcode
    }] }
    let(:enum_info) { 'v.2' }
    let(:limited_multivolume) { [{
                                id: item_2_id,
                                status: not_charged,
                                on_reserve: 'N',
                                temp_location: nil,
                                perm_location: 'num',
                                enum: enum_info,
                                chron: nil,
                                copy_number: 1,
                                item_sequence_number: 2,
                                status_date: '2014-05-27T06:00:19.000-05:00',
                                barcode: item_barcode
    }] }
    let(:volume) { 'vol. 24' }
    let(:chron_info) { 'Jan 2016' }
    let(:enum_with_chron) { [{
                                id: item_id,
                                status: not_charged,
                                on_reserve: 'N',
                                temp_location: nil,
                                perm_location: 'mus',
                                enum: volume,
                                chron: chron_info,
                                copy_number: 1,
                                item_sequence_number: 1,
                                status_date: '2014-05-27T06:00:19.000-05:00',
                                barcode: item_barcode
    }] }
    let(:temp) { 'scires' }
    let(:reserve_item) { [{
                                id: item_3_id,
                                status: not_charged,
                                on_reserve: 'Y',
                                temp_location: temp,
                                perm_location: 'sci',
                                enum: nil,
                                chron: nil,
                                copy_number: 1,
                                item_sequence_number: nil,
                                status_date: '2014-05-27T06:00:19.000-05:00',
                                barcode: item_barcode
    }] }
    let(:perm) { 'sciterm' }
    let(:temp_no_reserve) { [{
                                id: item_id,
                                status: not_charged,
                                on_reserve: 'N',
                                temp_location: temp,
                                perm_location: perm,
                                enum: nil,
                                chron: nil,
                                copy_number: 1,
                                item_sequence_number: 1,
                                status_date: '2014-05-27T06:00:19.000-05:00',
                                barcode: item_barcode
    }] }
    let(:three_items) { [enum_with_chron.first, reserve_item.first, limited_multivolume.first] }
    let(:charged_item_no_reserve) { [{
                                id: item_id,
                                status: charged,
                                on_reserve: 'N',
                                temp_location: nil,
                                perm_location: perm,
                                enum: nil,
                                chron: nil,
                                copy_number: 1,
                                item_sequence_number: 1,
                                status_date: '2014-05-27T06:00:19.000-05:00',
                                barcode: item_barcode,
                                due_date: Time.parse('2037-06-15 23:00:00 -0400')
    }] }
    let(:charged_item_reserve) { [{
                                id: item_id,
                                status: charged,
                                on_reserve: 'Y',
                                temp_location: temp,
                                perm_location: 'sci',
                                enum: nil,
                                chron: nil,
                                copy_number: 1,
                                item_sequence_number: 1,
                                status_date: '2014-05-27T06:00:19.000-05:00',
                                barcode: item_barcode,
                                due_date: Time.parse('2037-06-15 23:00:00 -0400')
    }] }
    let(:charged_item_long_overdue) { [{
                                id: item_id,
                                status: charged,
                                on_reserve: 'N',
                                temp_location: nil,
                                perm_location: perm,
                                enum: nil,
                                chron: nil,
                                copy_number: 1,
                                item_sequence_number: 1,
                                status_date: '2014-05-27T06:00:19.000-05:00',
                                barcode: item_barcode,
                                due_date: Time.parse('2000-06-15 23:00:00 -0400')
    }] }
    it 'includes item id and barcode in response' do
      allow(described_class).to receive(:get_items_for_holding).and_return(single_volume_2_copy)
      availability = described_class.get_full_mfhd_availability(placeholder_id).first
      expect(availability[:id]).to eq item_id
      expect(availability[:barcode]).to eq item_barcode
    end
    it 'includes Voyager status' do
      allow(described_class).to receive(:get_items_for_holding).and_return(single_volume_2_copy)
      availability = described_class.get_full_mfhd_availability(placeholder_id).first
      expect(availability[:status]).to eq not_charged
    end
    it 'includes enumeration info when present' do
      allow(described_class).to receive(:get_items_for_holding).and_return(limited_multivolume)
      availability = described_class.get_full_mfhd_availability(placeholder_id).first
      expect(availability[:enum]).to eq enum_info
    end
    it 'excludes enum when item enumeration is nil' do
      allow(described_class).to receive(:get_items_for_holding).and_return(single_volume_2_copy)
      availability = described_class.get_full_mfhd_availability(placeholder_id).first
      expect(availability[:enum]).to eq nil
    end
    it 'includes chron date with enumeration info when present' do
      allow(described_class).to receive(:get_items_for_holding).and_return(enum_with_chron)
      availability = described_class.get_full_mfhd_availability(placeholder_id).first
      expect(availability[:enum_display]).to include("(#{chron_info})")
    end
    it 'includes copy number for non-reserve items if value is not 1' do
      allow(described_class).to receive(:get_items_for_holding).and_return(single_volume_2_copy)
      availability = described_class.get_full_mfhd_availability(placeholder_id).first
      expect(availability[:copy_number]).to eq 2
    end
    it 'includes copy number regardless of value for on reserve item' do
      allow(described_class).to receive(:get_items_for_holding).and_return(reserve_item)
      availability = described_class.get_full_mfhd_availability(placeholder_id).first
      expect(availability[:copy_number]).to eq 1
    end
    it 'includes temp_location code for on reserve item' do
      allow(described_class).to receive(:get_items_for_holding).and_return(reserve_item)
      availability = described_class.get_full_mfhd_availability(placeholder_id).first
      expect(availability[:temp_loc]).to eq temp
    end
    it 'temp_location returned if present, including for non-reserve items' do
      allow(described_class).to receive(:get_items_for_holding).and_return(temp_no_reserve)
      availability = described_class.get_full_mfhd_availability(placeholder_id).first
      expect(availability[:temp_loc]).to eq temp
    end
    it 'sorts multiple items by item sequence number in reverse, nil last' do
      allow(described_class).to receive(:get_items_for_holding).and_return(three_items)
      availability = described_class.get_full_mfhd_availability(placeholder_id)
      item_ids = availability.map { |i| i[:id] }
      expect(item_ids).to eq [item_2_id, item_id, item_3_id]
    end
    it 'displays an item due date in the format mm/dd/yyyy (month and day no-padded) if charged, not on reserve, and not long overdue' do
      allow(described_class).to receive(:get_items_for_holding).and_return(charged_item_no_reserve)
      availability = described_class.get_full_mfhd_availability(placeholder_id).first
      expect(availability[:due_date]).to eq '6/15/2037'
    end
    it 'displays an item due date in the format mm/dd/yyyy (month and day no-padded) if charged, and long overdue' do
      allow(described_class).to receive(:get_items_for_holding).and_return(charged_item_long_overdue)
      availability = described_class.get_full_mfhd_availability(placeholder_id).first
      expect(availability[:due_date]).to eq nil
    end
    it 'displays an item due date in the format mm/dd/yyyy hh:mm(pm/am)(month, day, hour no-padded) if charged and on reserve' do
      allow(described_class).to receive(:get_items_for_holding).and_return(charged_item_reserve)
      availability = described_class.get_full_mfhd_availability(placeholder_id).first
      expect(availability[:due_date]).to eq '6/15/2037 11:00pm'
    end

    it 'displays an item on reserve flag marked yes' do
      allow(described_class).to receive(:get_items_for_holding).and_return(charged_item_reserve)
      availability = described_class.get_full_mfhd_availability(placeholder_id).first
      expect(availability[:on_reserve]).to eq 'Y'
    end
  end

  describe '#valid_ascii' do
    it 'strips out non-ASCII characters and converts to UTF-8' do
      expect(described_class.send(:valid_ascii, 'a¯')).to eq 'a'
    end
  end

  describe '#valid_codepoints' do
    it 'Converts valid UTF-8 codepoints stored in an ASCII field to UTF-8 string' do
      expect(described_class.send(:valid_codepoints, 'Schröder')).to eq 'Schröder'
    end
  end

  describe 'holding and item merge related methods' do
    let(:norm_bib_id) { '7991903' }
    let(:norm_bib_record) { json_stub("bib_#{norm_bib_id}") }
    let(:norm_mfhd_id) { '7770428' }
    let(:norm_mfhd_record) { json_stub("mfhd_#{norm_mfhd_id}") }
    describe '#merge_holdings_info' do
      let(:cat_date) { "2014-02-07 09:34:47 -0500" }
      it 'merges mfhd information into a bib record and adds catalog date' do
        allow(described_class).to receive(:get_catalog_date).and_return(cat_date)
        record_hash = described_class.send(:merge_holdings_info, norm_bib_record, [norm_mfhd_record])
        record_marc = MARC::Record.new_from_hash(record_hash)
        expect(record_marc['852']['0']).to eq norm_mfhd_id
        expect(record_marc['852']['h']).to eq 'PS3561.R2873'
        expect(record_marc['852']['i']).to eq 'A68 2013'
        expect(record_marc['959']['a']).to eq cat_date
      end
    end
    describe '#merge_holding_item_into_bib' do
      let(:norm_merged_mfhd_record) { json_stub("merged_mfhd_#{norm_bib_id}") }
      let(:norm_barcode) { '32101089814220' }
      let(:norm_item_id) { '6800460' }
      let(:norm_item_info)  {{
                              :id=>norm_item_id,
                              :status=>"Not Charged",
                              :on_reserve=>"N",
                              :copy_number=>1,
                              :item_sequence_number=>1,
                              :temp_location=>nil,
                              :perm_location=>"f",
                              :enum=>nil,
                              :chron=>nil,
                              :status_date=>DateTime.new(2016,10,19,20,21,25,'-5'),
                              :barcode=>norm_barcode
                            }}
      let(:null_item_enum_with_chron)  {{
                              :id=>norm_item_id,
                              :status=>"Not Charged",
                              :on_reserve=>"N",
                              :copy_number=>1,
                              :item_sequence_number=>1,
                              :temp_location=>nil,
                              :perm_location=>"f",
                              :enum=>nil,
                              :chron=>"1992",
                              :status_date=>DateTime.new(2016,10,19,20,21,25,'-5'),
                              :barcode=>norm_barcode
                            }}
      let(:item_enum_with_null_chron)  {{
                              :id=>norm_item_id,
                              :status=>"Not Charged",
                              :on_reserve=>"N",
                              :copy_number=>1,
                              :item_sequence_number=>1,
                              :temp_location=>nil,
                              :perm_location=>"f",
                              :enum=>"v.1",
                              :chron=>nil,
                              :status_date=>DateTime.new(2016,10,19,20,21,25,'-5'),
                              :barcode=>norm_barcode
                            }}
      let(:recap_bib_id) { '159315' }
      let(:recap_bib_record) { json_stub("bib_#{recap_bib_id}") }
      let(:recap_mfhd_id) { '176124' }
      let(:recap_mfhd_record) { json_stub("mfhd_#{recap_mfhd_id}") }
      let(:recap_merged_mfhd_record) { json_stub("merged_mfhd_#{recap_bib_id}") }
      let(:recap_barcode) { '32101063503237' }
      let(:recap_item_id) { '171815' }
      let(:recap_item_info)  {{
                              :id=>recap_item_id,
                              :status=>"Not Charged",
                              :on_reserve=>"N",
                              :copy_number=>1,
                              :item_sequence_number=>1,
                              :temp_location=>nil,
                              :perm_location=>"rcppa",
                              :enum=>"v.2",
                              :chron=>"1948",
                              :status_date=>DateTime.new(2011,10,19,20,21,25,'-5'),
                              :barcode=>recap_barcode
                            }}
      context 'non-ReCAP item, ReCAP flag off' do
        it 'retains 852$h and $i and adds item info to 876 without ReCAP-specific fields' do
          allow(described_class).to receive(:merge_holdings_info).and_return(norm_merged_mfhd_record.to_hash)
          full_record = described_class.send(:merge_holding_item_into_bib, norm_bib_record, norm_mfhd_record, norm_item_info)
          expect(full_record['852']['i']).to eq 'A68 2013'
          expect(full_record['876']['0']).to eq norm_mfhd_id
          expect(full_record['876']['a']).to eq norm_item_id
          expect(full_record['876']['j']).to eq 'Not Charged'
          expect(full_record['876']['p']).to eq norm_barcode
          expect(full_record['876']['t']).to eq '1'
          expect(full_record['876']['x']).to be_nil
        end
      end
      context 'non-ReCAP item, ReCAP flag off, enum without chron' do
        it 'has enum in 876$3' do
          allow(described_class).to receive(:merge_holdings_info).and_return(norm_merged_mfhd_record.to_hash)
          full_record = described_class.send(:merge_holding_item_into_bib, norm_bib_record, norm_mfhd_record, item_enum_with_null_chron)
          expect(full_record['876']['3']).to eq 'v.1'
        end
      end
      context 'non-ReCAP item, ReCAP flag off, chron without enum' do
        it 'has chron without parens in 876$3' do
          allow(described_class).to receive(:merge_holdings_info).and_return(norm_merged_mfhd_record.to_hash)
          full_record = described_class.send(:merge_holding_item_into_bib, norm_bib_record, norm_mfhd_record, null_item_enum_with_chron)
          expect(full_record['876']['3']).to eq '1992'
        end
      end
      context 'non-ReCAP item, ReCAP flag on' do
        it 'retains 852$h and $i and adds item info to 876 without ReCAP-specific fields' do
          allow(described_class).to receive(:merge_holdings_info).and_return(norm_merged_mfhd_record.to_hash)
          full_record = described_class.send(:merge_holding_item_into_bib, norm_bib_record, norm_mfhd_record, norm_item_info, recap=true)
          expect(full_record['852']['i']).to eq 'A68 2013'
          expect(full_record['876']['0']).to eq norm_mfhd_id
          expect(full_record['876']['a']).to eq norm_item_id
          expect(full_record['876']['j']).to eq 'Not Charged'
          expect(full_record['876']['p']).to eq norm_barcode
          expect(full_record['876']['t']).to eq '1'
          expect(full_record['876']['x']).to be_nil
        end
      end
      context 'ReCAP item, ReCAP flag on, with enum and chron' do
        it 'merges 852$h and $i into 852 $h and adds ReCAP-specific info to 876' do
          allow(described_class).to receive(:merge_holdings_info).and_return(recap_merged_mfhd_record.to_hash)
          full_record = described_class.send(:merge_holding_item_into_bib, recap_bib_record, recap_mfhd_record, recap_item_info, recap=true)
          expect(full_record['852']['h']).to eq 'BM899.61 .A39 M8'
          expect(full_record['876']['0']).to eq recap_mfhd_id
          expect(full_record['876']['3']).to eq 'v.2 (1948)'
          expect(full_record['876']['a']).to eq recap_item_id
          expect(full_record['876']['j']).to eq 'Not Charged'
          expect(full_record['876']['p']).to eq recap_barcode
          expect(full_record['876']['t']).to eq '1'
          expect(full_record['876']['x']).to eq 'Shared'
        end
      end
      context 'ReCAP item, ReCAP flag off' do
        it 'retains 852$h and $i into 852 $h and does not add ReCAP-specific info to 876' do
          allow(described_class).to receive(:merge_holdings_info).and_return(recap_merged_mfhd_record.to_hash)
          full_record = described_class.send(:merge_holding_item_into_bib, recap_bib_record, recap_mfhd_record, recap_item_info, recap=false)
          expect(full_record['852']['i']).to eq '.A39 M8'
          expect(full_record['876']['0']).to eq recap_mfhd_id
          expect(full_record['876']['3']).to eq 'v.2 (1948)'
          expect(full_record['876']['p']).to eq recap_barcode
          expect(full_record['876']['x']).to be_nil
        end
      end
    end
  end

  describe '#recap_item_info' do
    let(:shared) { 'rcppa' }
    let(:private_no_restriction) { 'rcpjq' }
    let(:private_in_library) { 'rcppj' }
    let(:private_supervised) { 'rcppb' }
    let(:invalid_recap) { 'rcppv' }
    context 'shared location, no use restriction' do
      it 'returns customer code, shared group designation, and blank use restriction' do
        info_hash = described_class.send(:recap_item_info, shared)
        expect(info_hash[:customer_code]).to eq 'PA'
        expect(info_hash[:recap_use_restriction]).to eq ''
        expect(info_hash[:group_designation]).to eq 'Shared'
      end
    end
    context 'private location, no use restriction' do
      it 'returns customer code, shared group designation, and blank use restriction' do
        info_hash = described_class.send(:recap_item_info, private_no_restriction)
        expect(info_hash[:customer_code]).to eq 'JQ'
        expect(info_hash[:recap_use_restriction]).to eq ''
        expect(info_hash[:group_designation]).to eq 'Private'
      end
    end
    context 'private location, in library use' do
      it 'returns customer code, shared group designation, and use restriction' do
        info_hash = described_class.send(:recap_item_info, private_in_library)
        expect(info_hash[:customer_code]).to eq 'PJ'
        expect(info_hash[:recap_use_restriction]).to eq 'In Library Use'
        expect(info_hash[:group_designation]).to eq 'Private'
      end
    end
    context 'private location, supervised use' do
      it 'returns customer code, shared group designation, and blank use restriction' do
        info_hash = described_class.send(:recap_item_info, private_supervised)
        expect(info_hash[:customer_code]).to eq 'PB'
        expect(info_hash[:recap_use_restriction]).to eq 'Supervised Use'
        expect(info_hash[:group_designation]).to eq 'Private'
      end
    end
    context 'invalid ReCAP location' do
      it 'returns customer code, blank group designation, and blank use restriction' do
        info_hash = described_class.send(:recap_item_info, invalid_recap)
        expect(info_hash[:customer_code]).to eq 'PV'
        expect(info_hash[:recap_use_restriction]).to eq ''
        expect(info_hash[:group_designation]).to eq ''
      end
    end
  end
end
