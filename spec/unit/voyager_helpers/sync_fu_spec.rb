require 'spec_helper'

describe VoyagerHelpers::SyncFu do

  let(:fixtures_dir) { File.join(File.dirname(__FILE__), '../../fixtures') }
  let(:earlier_file) { File.join(fixtures_dir, 'earlier_bib_mfhd_dump.txt') }
  let(:later_file) { File.join(fixtures_dir, 'later_bib_mfhd_dump.txt') }
  let(:subject) { described_class }


  describe '#compare_id_dumps' do

    it 'produces updated and deleted IDs as expected' do
      report = subject.compare_id_dumps(earlier_file, later_file)
      expected_updated = %w[3 4 10]
      expected_deleted = %w[7 8]
      expect(report.updated).to eq expected_updated
      expect(report.deleted).to eq expected_deleted
    end

  end

end
