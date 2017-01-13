require 'spec_helper'

RSpec.describe VoyagerHelpers::AvailabilityStatus do
  describe "#status_label" do
    ['Lost--system applied'].each do |status|
      it "is Long overdue for #{status}" do
        status_obj = build_status(status)

        expect(status_obj.status_label).to eq "Long overdue"
      end
    end
    ['Lost--library applied'].each do |status|
      it "is Lost for #{status}" do
        status_obj = build_status(status)

        expect(status_obj.status_label).to eq "Lost"
      end
    end
    ['Not charged', 'On shelf'].each do |status|
      it "is Available for #{status}" do
        status_obj = build_status(status)

        expect(status_obj.status_label).to eq "Available"
      end
    end
    ['Discharged'].each do |status|
      it "is Returned for #{status}" do
        status_obj = build_status(status)

        expect(status_obj.status_label).to eq "Returned"
      end
    end
    ['In transit discharged'].each do |status|
      it "is In transit for #{status}" do
        status_obj = build_status(status)

        expect(status_obj.status_label).to eq "In transit"
      end
    end
    ['In process', 'On-site - in process'].each do |status|
      it "is In process for #{status}" do
        status_obj = build_status(status)

        expect(status_obj.status_label).to eq "In process"
      end
    end
    ['Charged', 'Renewed', 'Overdue', 'On hold',
     'In transit', 'In transit on hold', 'At bindery',
     'Remote storage request', 'Hold request', 'Recall request'].each do |status|
       it "is Checked out for #{status}" do
         status_obj = build_status(status)

         expect(status_obj.status_label).to eq "Checked out"
       end
     end
     ['Missing', 'Claims returned', 'Withdrawn'].each do |status|
       it "is Missing for #{status}" do
         status_obj = build_status(status)

         expect(status_obj.status_label).to eq "Missing"
       end
     end
     it "returns See front desk if it contains On-site - " do
       status_obj = build_status("Something On-site - Sorta")

       expect(status_obj.status_label).to eq "See front desk"
     end
     it "returns On-site access if it contains On-site" do
       status_obj = build_status("Something On-site But No Dash")

       expect(status_obj.status_label).to eq "On-site access"
     end
     it "returns Order received if it contains Order received" do
       status_obj = build_status("Some Order received on a date")

       expect(status_obj.status_label).to eq "Order received"
     end
     it "returns Pending order if it contains Pending order" do
       status_obj = build_status("A Pending order due at some point")

       expect(status_obj.status_label).to eq "Pending order"
     end
     it "returns On-order if it contains On-order" do
       status_obj = build_status("Something On-order due sometime")

       expect(status_obj.status_label).to eq "On-order"
     end
  end

  def build_status(status)
    described_class.new({status: status})
  end
end
