module TopNRecords
  def self.parse_join(klass, join)
    case join
    when Hash
      raise ArgumentError, 'invalid join' unless join.size == 1
      join.first
    when Symbol
      [klass.primary_key, join_condition]
    when NilClass
      [klass.primary_key, klass.name.foreign_key]
    else
      raise ArgumentError, 'invalid join'
    end
  end

  def self.parse_order(klass, order)
    case order
    when Hash
      raise ArgumentError, 'invalid order' unless order.size == 1 && %i[asc desc].include?(order.first.last)
      order.first
    when Symbol
      [order, :asc]
    when NilClass
      [target_klass.primary_key, :asc]
    else
      raise ArgumentError, 'invalid order'
    end
  end

  def self.top_n_records(klass, primary_keys, target_klass, limit:, join: nil, order: nil, condition: nil)
    primary_key, foreign_key = parse_join klass, join
    order_key, order_mode = parse_order klass, order

    sql = TopNRecords::SQLBuilder.top_n_sql(
      table_name: klass.table_name,
      target_table_name: target_klass.table_name,
      primary_key: primary_key,
      foreign_key: foreign_key,
      order_mode: order_mode,
      order_key: order_key,
      condition: condition
    )
    records = target_klass.find_by_sql([sql, primary_keys: primary_keys, offset: limit])
    result = Hash.new { [] }.merge(records.group_by { |o| o[foreign_key] })
    order_sub_key = target_klass.primary_key
    result.transform_values do |grouped_records|
      existings, blanks = grouped_records.partition { |o| o[order_key] }
      existings.sort_by! { |o| [o[order_key], o[order_sub_key]] }
      blanks.sort_by! { |o| o[order_sub_key] }
      ordered = existings + blanks
      ordered.reverse! if order_mode == :desc
      ordered.take limit
    end
  end

  module SQLBuilder
    def self.top_n_sql(table_name:, target_table_name:, primary_key:, foreign_key:, order_mode:, order_key:, condition:)
      order_op = order_mode == :asc ? :< : :>
      condition_sql = where_condition_to_sql condition
      %(
        SELECT *
        FROM (
          SELECT "#{table_name}"."#{primary_key}" AS key,
          (
            SELECT "#{target_table_name}"."#{order_key}" FROM "#{target_table_name}"
            WHERE "#{target_table_name}"."#{foreign_key}" = "#{table_name}"."#{primary_key}"
            #{"AND #{condition_sql}" if condition_sql}
            ORDER BY "#{target_table_name}"."#{order_key}" #{order_mode.to_s.upcase}
            LIMIT 1 OFFSET :offset
          ) AS last_value
          FROM "#{table_name}"
          WHERE "#{table_name}"."#{primary_key}" IN (:primary_keys)
        ) T
        INNER JOIN "#{target_table_name}" ON
          "#{target_table_name}"."#{foreign_key}" = T.key AND
          (T.last_value IS NULL OR "#{target_table_name}"."#{order_key}" #{order_op} T.last_value)
        #{"WHERE #{condition_sql}" if condition_sql}
      )
    end

    def self.where_condition_to_sql(condition)
      case condition
      when String
        condition
      when Array
        ActiveRecord::Base.send :sanitize_sql_array, condition
      when Hash
        condition.map { |key, value| kv_condition_to_sql key, value }.join ' AND '
      end
    end

    def self.kv_condition_to_sql(key, value)
      sql_binds = begin
        case value
        when NilClass
          %("#{key}" IS NULL)
        when Range
          sql = value.exclude_end? ? %("#{key}" >= ? AND "#{key} < ?) : %("#{key}" BETWEEN ? AND ?)
          [sql, value.begin, value.end]
        when Enumerable
          [%("#{key}" IN (?)), value.to_a]
        else
          [%("#{key}" IS ?), value]
        end
      end
      ActiveRecord::Base.send :sanitize_sql_array, sql_binds
    end
  end
end

class ActiveRecord::Relation
  def top_n_child_records(target_klass, option = {})
    primary_keys = loaded? ? map { |o| o[primary_key] } : pluck(primary_key)
    TopNRecords.top_n_records klass, primary_keys, target_klass, option
  end
end

User.first.post_ids # => [1, 3, 9, 10, 15, 18, 19]
User.first.posts.top_n_child_records(Comment, limit: 2, order: { created_at: :desc }, condition: { id: (1..16) })
__END__
{15=>
  [#<Comment:0x00007fe34f2718b8 id: 14, post_id: 15, user_id: 3>,
   #<Comment:0x00007fe34f2710c0 id: 5, post_id: 15, user_id: 2>],
 3=>
  [#<Comment:0x00007fe34f2714a8 id: 12, post_id: 3, user_id: 1>],
 1=>
  [#<Comment:0x00007fe34f2712c8 id: 11, post_id: 1, user_id: 4>,
   #<Comment:0x00007fe34f270e68 id: 2, post_id: 1, user_id: 4>]}
