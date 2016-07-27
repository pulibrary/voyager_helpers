module VoyagerHelpers
  class Course < Struct.new(:reserve_list_id, :department_name, :course_name, :course_number, :section_id, :instructor_first_name, :instructor_last_name)
  end
end
