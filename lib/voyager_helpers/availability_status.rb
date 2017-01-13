module VoyagerHelpers
  class AvailabilityStatus
    attr_reader :availability_info
    def initialize(availability_info)
      @availability_info = availability_info
    end

    def status_label
      hash_label || regex_label || status
    end

    def status
      availability_info[:status]
    end

    private

    def hash_label
      Array(status_label_hash.find { |k, _| k.include?(status) }).last
    end

    def regex_label
      Array(regex_hash.find { |k, _| status.match(k) }).last
    end

    def regex_hash
      {
        /On-site - / => 'See front desk',
        /On-site/ => 'On-site access',
        /Order received/ => 'Order received',
        /Pending order/ => 'Pending order',
        /On-order/ => 'On-order'
      }
    end

    def status_label_hash
      {
        ['Lost--system applied'] => 'Long overdue',
        ['Lost--library applied'] => 'Lost',
        ['Not charged', 'On shelf'] => 'Available',
        ['Discharged'] => 'Returned',
        ['In transit discharged'] => 'In transit',
        ['In process', 'On-site - in process'] => 'In process',
        ['Charged', 'Renewed', 'Overdue', 'On hold',
         'In transit', 'In transit on hold', 'At bindery',
         'Remote storage request', 'Hold request', 'Recall request'] => 'Checked out',
        ['Missing', 'Claims returned', 'Withdrawn'] => 'Missing'
      }
    end
  end
end
