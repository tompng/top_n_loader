require "test_helper"

class TopNLoaderTest < Minitest::Test
  def test_version
    refute_nil ::TopNLoader::VERSION
  end

  def expected_groups_result(records, key, limit)
    records.group_by(&key).map do |key, list|
      [
        key.is_a?(ActiveRecord::Base) ? key[key.class.primary_key] : key,
        list.take(limit)
      ]
    end.to_h
  end

  def expected_associations_result(klass, ids, relation, limit)
    klass.find(ids).map do |a|
      [a.id, a.send(relation).order(id: :asc).limit(limit)]
    end.reject { |_k, v| v.empty? }.to_h
  end

  def test_valid_seed
    assert_equal 100, Normal.count
    assert_equal 100, Sti.count
    DB::VALUES.each do |key, values|
      assert_equal Normal.count, Normal.where(key => values).count
      assert_equal Sti.count, Sti.where(key => values).count
    end
    assert_equal Sti.count, Sti.where(type: DB::TYPES).count
  end

  def test_belongs_to_reflections
    %i[foo bar].each do |relation|
      expected = Normal.find(1, 2, 3).map { |n| [n.id, [n.send(relation)]]}.to_h
      result = TopNLoader.load_associations Normal, [1, 2, 3], relation, limit: 8
      assert_equal expected, result, relation
    end
  end

  def test_has_many_reflections
    [1, 8].each do |limit|
      %i[bars normals stis stias large_normals].each do |relation|
        expected = expected_associations_result Foo, [1, 2, 3], relation, limit
        result = TopNLoader.load_associations Foo, [1, 2, 3], relation, limit: limit
        assert_equal expected, result, relation
      end
    end
  end

  def test_self_join
    [1, 8].each do |limit|
      expected = expected_associations_result Bar, [1, 2, 3], :normal_same_id_foo_bars, limit
      result = TopNLoader.load_associations Bar, [1, 2, 3], :normal_same_id_foo_bars, limit: limit
      assert_equal expected, result
      expected = expected_associations_result Bar, [1, 2, 3], :normal_same_id_foo_bar_singularized, limit
      result = TopNLoader.load_associations Bar, [1, 2, 3], :normal_same_id_foo_bar_singularized, limit: limit
      assert_equal expected, result
    end
  end

  def test_including_self_join
    [1, 8].each do |limit|
      expected = expected_associations_result Normal, [1, 2, 3], :bar_normal_same_id_foo_bars, limit
      result = TopNLoader.load_associations Normal, [1, 2, 3], :bar_normal_same_id_foo_bars, limit: limit
      assert_equal expected, result
    end
  end

  def test_reflection_explain
    [1, 3].each do |limit|
      sql = TopNLoader::SQLBuilder.top_n_association_sql Foo, Bar, :bars, limit: limit, order_mode: :asc, order_key: :id
      explain = Bar.exec_explain [[sql, []]]
      assert !explain.include?('SCAN TABLE'), explain
    end
  end

  def test_group_explain
    [1, 3].each do |limit|
      sql, = TopNLoader::SQLBuilder.top_n_group_sql(
        klass: Normal,
        group_column: :int,
        group_keys: [1, 2, 3],
        condition: nil,
        limit: limit,
        order_mode: :asc,
        order_key: :id
      )
      explain = Normal.exec_explain([[sql, []]])
      assert !explain.include?('SCAN TABLE'), explain
    end
  end

  def test_combinations
    classes = [Normal, Sti, StiA, StiB, StiAA, StiAB, StiAAB]
    column_values_list = DB::VALUES.flat_map do |key, values|
      include_nils = 2.times.map { [key, (values - [nil]).sample(3) + [nil]] }
      exclude_nils = 2.times.map { [key, (values - [nil]).sample(4)] }
      include_nils + exclude_nils
    end
    orders = [
      [nil, { id: :asc }],
      [:asc, { id: :asc }],
      [:desc, { id: :desc }],
      [{ string: :asc }, { string: :asc, id: :asc }],
      [{ string: :desc }, { string: :desc, id: :desc }]
    ]
    limits = [1, 2, 32]
    classes.product column_values_list, orders, limits do |klass, (column, values), (order, ar_order), limit|
      records = klass.where(column => values).order(ar_order)
      result = TopNLoader.load_groups klass, column, values, order: order, limit: limit
      expected = expected_groups_result records, column, limit
      message = "#{klass}, #{column}: #{values.inspect}, order: #{order}, limit: #{limit}"
      assert_equal expected, result, message
    end
  end

  def test_errors
    TopNLoader.load_groups Normal, :int, [1, 2, 3], limit: 3
    TopNLoader.load_groups Normal, :int, [1, 2, 3], order: :desc, limit: 3
    TopNLoader.load_groups Normal, :int, [1, 2, 3], order: { string: :desc }, limit: 3
    empty_hash = {}
    assert_equal empty_hash, TopNLoader.load_groups(Normal, :int, [], limit: 3)
    assert_equal empty_hash, TopNLoader.load_groups(Normal, :int, [1, 2, 3], limit: 0)
    assert_equal [], TopNLoader.load_groups(Normal, :int, [1, 2, 3], limit: 9)[4]
    assert_equal [], TopNLoader.load_groups(Normal, :int, [1, 2, 3], limit: 3)[4]
    assert_raises(ArgumentError) { TopNLoader.load_groups Normal, :int, [1, 2, 3], limit: -1 }
    assert_raises(ArgumentError) { TopNLoader.load_groups Normal, :int, [1, 2, 3], order: :desk, limit: 3 }
    assert_raises(ArgumentError) { TopNLoader.load_groups Normal, :int, [1, 2, 3], order: { string: :desk }, limit: 3 }
    assert_raises(ArgumentError) { TopNLoader.load_groups Normal, :int, [1, 2, 3], order: :desc }
  end

  def test_conditions
    ints = DB::VALUES[:int]
    string_include = DB::VALUES[:string].sample(4) + [nil]
    id_exclude = (10..30)
    date_not = DB::VALUES[:date].compact.sample
    top_n_condition1 = { string: string_include, id: { not: id_exclude }, not: { date: date_not } }
    top_n_condition2 = { string: string_include, not: { id: id_exclude }, date: { not: date_not } }
    records = Normal.where(string: string_include).where.not(id: id_exclude).where.not(date: date_not).order(id: :desc)
    [1, 2, 32].each do |limit|
      expected = expected_groups_result(records, :int, limit)
      result1 = TopNLoader.load_groups Normal, :int, ints, order: :desc, limit: limit, condition: top_n_condition1
      result2 = TopNLoader.load_groups Normal, :int, ints, order: :desc, limit: limit, condition: top_n_condition2
      assert_equal result1, result2
      assert_equal expected, result1
      assert_equal expected, result2
    end
  end
end
