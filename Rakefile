#!/usr/bin/env rake

require "bundler/gem_tasks"
require 'rspec/core/rake_task'

RSpec::Core::RakeTask.new(:all_specs)

RSpec::Core::RakeTask.new(:spec) do |task|
  task.rspec_opts = '--tag ~skip_ci'
end

task :ci do
  Rake::Task['spec'].invoke
end

task default: :ci
