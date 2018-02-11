class ActiveRecord::Relation
  def top_n_child_records target_klass, limit, join: nil, order: nil, sql: nil
    primary_key = klass.primary_key
    if join.is_a? Hash
      raise unless order.size == 1
      primary_key, foreign_key = join.to_a.first
    elsif join.is_a? Symbol
      foreign_key = join_condition
    end
    foreign_key ||= klass.name.foreign_key

    condition_sql = ActiveRecord::Base.send :sanitize_sql_array, sql if sql

    order_key = target_klass.primary_key
    order_mode = :asc
    if order.is_a? Hash
      raise unless order.size == 1
      order_key, order_mode = order.to_a.first
    elsif order.is_a? Symbol
      order_key = order
    end

    if order_mode == :asc
      order_op = :<
    elsif order_mode = :desc
      order_op = :>
    else
      raise
    end

    table_name = klass.table_name
    target_table_name = target_klass.table_name
    target_klass.find_by_sql([
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
        ORDER BY "#{target_table_name}"."#{order_key}" #{order_mode.to_s.upcase}
      ),
      {
        primary_keys: loaded? ? map { |record| record[primary_key] } : pluck(primary_key),
        offset: limit
      }
    ]).group_by { |record| record[foreign_key] }
  end
end

User.first.post_ids # => [1, 3, 9, 10, 15, 18, 19]
User.first.posts.top_n_child_records(Comment, 2, order: { created_at: :desc }, sql: ['id < ?', 16])
__END__
{15=>
  [#<Comment:0x00007fe34f2718b8 id: 14, post_id: 15, user_id: 3>,
   #<Comment:0x00007fe34f2710c0 id: 5, post_id: 15, user_id: 2>],
 3=>
  [#<Comment:0x00007fe34f2714a8 id: 12, post_id: 3, user_id: 1>],
 1=>
  [#<Comment:0x00007fe34f2712c8 id: 11, post_id: 1, user_id: 4>,
   #<Comment:0x00007fe34f270e68 id: 2, post_id: 1, user_id: 4>]}
