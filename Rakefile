#!/usr/bin/env rake

require "bundler/gem_tasks"
require 'rspec/core/rake_task'

RSpec::Core::RakeTask.new(:spec)

RSpec::Core::RakeTask.new(:ci_specs) do |task|
  task.rspec_opts = '--tag ~skip_ci'
end

task :ci do
  Rake::Task['ci_specs'].invoke
end

task default: :ci
