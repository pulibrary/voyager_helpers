require 'spec_helper'

describe VoyagerHelpers::Liberator do
  let(:placeholder_id) { 12345 }

  describe '#get_order_status' do
    let(:date) { Date.parse("2015-12-14T15:34:00.000-05:00") }
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
                        date: Date.parse("2015-12-16T15:34:00.000-05:00"),
                        li_status: 8,
                        po_status: 3
                        }] }
    let(:received_order) { [{
                        date: Date.parse("2015-12-15T15:34:00.000-05:00"),
                        li_status: 1,
                        po_status: 4
                        }] }

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
  end

  describe '#get_full_mfhd_availability' do
    let(:item_id) { 36736 }
    let(:item_barcode) { '32101005535917' }
    let(:not_charged) { 'Not Charged' }
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
                                id: item_id,
                                status: not_charged,
                                on_reserve: 'N',
                                temp_location: nil,
                                perm_location: 'num',
                                enum: enum_info,
                                chron: nil,
                                copy_number: 1,
                                item_sequence_number: 1,
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
                                id: item_id,
                                status: not_charged,
                                on_reserve: 'Y',
                                temp_location: temp,
                                perm_location: 'sci',
                                enum: nil,
                                chron: nil,
                                copy_number: 1,
                                item_sequence_number: 1,
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
      expect(availability[:enum]).to include("(#{chron_info})")
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
      expect(availability[:on_reserve]).to eq temp
    end
    it 'temp_location returned if present, including for non-reserve items' do
      allow(described_class).to receive(:get_items_for_holding).and_return(temp_no_reserve)
      availability = described_class.get_full_mfhd_availability(placeholder_id).first
      expect(availability[:on_reserve]).to eq temp
    end
  end

  describe '#valid_ascii' do
    it 'strips out non-ASCII characters and converts to UTF-8' do
      expect(described_class.send(:valid_ascii, 'a¯')).to eq 'a'
    end
  end

end
