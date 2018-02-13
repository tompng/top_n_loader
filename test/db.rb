require 'benchmark'
require 'active_record'

class Normal < ActiveRecord::Base; end
class Sti < ActiveRecord::Base; end
class StiA < Sti; end
class StiB < Sti; end
class StiAA < StiA; end
class StiAB < StiA; end
class StiAAB < StiAA; end
Class.new(StiA)

module DB
  DATABASE_CONFIG = {
    adapter: 'sqlite3',
    database: ENV['DATABASE_NAME'] || 'test/development.sqlite3',
    pool: 5,
    timeout: 5000
  }
  ActiveRecord::Base.establish_connection DATABASE_CONFIG
  ActiveRecord::Base.logger = Logger.new(STDOUT)

  def self.migrate
    File.unlink DATABASE_CONFIG[:database] if File.exist? DATABASE_CONFIG[:database]
    ActiveRecord::Base.clear_all_connections!
    ActiveRecord::Migration::Current.class_eval do
      create_table :normals do |t|
        t.string :string
        t.integer :int
        t.date :date
        t.timestamps
      end
      create_table :stis do |t|
        t.string :type
        t.string :string
        t.integer :int
        t.date :date
        t.timestamps
      end
    end
  end

  VALUES = {
    string: %w[hello world ruby active record] + [nil],
    int: [nil, *(1..10)],
    date: (1..12).map { |i| Date.new 2000, i, rand(1..28) } + [nil]
  }
  TYPES = %w[StiA StiB StiAA StiAB StiAAB] + [nil]

  def self.seed
    100.times do
      Normal.create VALUES.transform_values(&:sample)
      Sti.create type: TYPES.sample, **VALUES.transform_values(&:sample)
    end
  end
end
