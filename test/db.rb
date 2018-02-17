require 'benchmark'
require 'active_record'

class Foo < ActiveRecord::Base
  has_many :bars, foreign_key: :int
  has_many :normals, through: :bars
  has_many :stis, through: :bars
  has_many :stias, through: :bars, source: :stis, class_name: 'StiA'
  has_many :large_normals, -> { where id: 50..100 }, through: :bars, source: :normals
end
class Bar < ActiveRecord::Base
  belongs_to :foo, foreign_key: :int, required: false
  has_many :normals, foreign_key: :int
  has_many :stis, foreign_key: :int
  has_many :normal_same_id_foos, through: :normals, source: :foo_with_same_id
  has_many :normal_same_id_foo_bars, through: :normal_same_id_foos, source: :bars
end
class Normal < ActiveRecord::Base
  belongs_to :bar, foreign_key: :int, required: false
  has_one :foo, through: :bar
  has_one :foo_with_same_id, class_name: 'Foo', foreign_key: :id, primary_key: :id
end
class Sti < ActiveRecord::Base
  belongs_to :bar, foreign_key: :int, required: false
  has_one :foo, through: :bar
end
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
      create_table :foos do |t|
        t.string :string
        t.timestamps
      end
      create_table :bars do |t|
        t.string :string
        t.integer :int, index: true
        t.timestamps
      end
      create_table :normals do |t|
        t.string :string
        t.integer :int, index: true
        t.date :date
        t.timestamps
      end
      create_table :stis do |t|
        t.string :type
        t.string :string
        t.integer :int, index: true
        t.date :date
        t.timestamps
      end
    end
  end

  srand 1
  VALUES = {
    string: %w[hello world ruby active record] + [nil],
    int: [nil, *(1..10)],
    date: (1..12).map { |i| Date.new 2000, i, rand(1..28) } + [nil]
  }
  TYPES = %w[StiA StiB StiAA StiAB StiAAB] + [nil]

  def self.seed
    4.times { Foo.create string: VALUES[:string].sample }
    10.times { Bar.create string: VALUES[:string].sample, int: rand(1..4) }
    100.times do
      Normal.create VALUES.transform_values(&:sample)
      Sti.create type: TYPES.sample, **VALUES.transform_values(&:sample)
    end
  end
end
