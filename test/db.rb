require 'benchmark'
require 'active_record'

class Foo < ActiveRecord::Base
  has_many :bars, foreign_key: :int
  has_many :barses, foreign_key: :int, class_name: 'Bar'
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
  has_many :normal_same_id_foo_bar_singularized, through: :normal_same_id_foos, source: :barses
end
class Normal < ActiveRecord::Base
  belongs_to :bar, foreign_key: :int, required: false
  has_one :foo, through: :bar
  has_many :bar_normal_same_id_foo_bars, through: :bar, source: :normal_same_id_foo_bars
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

module DB
  DATABASE_CONFIG_SQLITE3 = {
    adapter: 'sqlite3',
    database: 'test/development.sqlite3',
    pool: 5,
    timeout: 5000
  }

  ActiveRecord::Base.logger = Logger.new(STDOUT)

  def self.connect(config)
    ActiveRecord::Base.establish_connection config
  end

  def self.migrate(config)
    if config[:adapter] == 'sqlite3'
      File.unlink config[:database] if File.exist? config[:database]
    else
      ActiveRecord::Tasks::DatabaseTasks.drop config
      ActiveRecord::Tasks::DatabaseTasks.create config
    end
    connect config
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
