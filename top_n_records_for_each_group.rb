class ActiveRecord::Relation
  def each_limit_group target_klass, join: nil, order: nil, limit: 1
    primary_key = klass.primary_key
    if join.is_a? Hash
      ((primary_key, foreign_key)) = join.to_a
    elsif join.is_a? Symbol
      foreign_key = join_condition
    end
    foreign_key ||= klass.name.foreign_key

    order_key = target_klass.primary_key
    order_mode = :asc
    if order.is_a? Hash
      ((order_key, order_mode)) = order.to_a
    elsif order.is_a? Symbol
      order_key = order
    end

    ordering = {
      'asc' => { mode: :ASC, op: '<' },
      'desc' => { mode: :DESC, op: '>' }
    }[order_mode.to_s.downcase]

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
            ORDER BY "#{target_table_name}"."#{order_key}" #{ordering[:mode]} LIMIT 1 OFFSET :offset
          ) AS last_value
          FROM "#{table_name}"
          WHERE "#{table_name}"."#{primary_key}" in (:primary_keys)
        ) T
        INNER JOIN "#{target_table_name}" ON
          "#{target_table_name}"."#{foreign_key}" = T.key AND
          (T.last_value IS NULL OR "#{target_table_name}"."#{order_key}" #{ordering[:op]} T.last_value)
        ORDER BY "#{target_table_name}"."#{order_key}" #{ordering[:mode]}
      ),
      {
        primary_keys: loaded? ? map { |record| record[primary_key] } : pluck(primary_key),
        offset: limit
      }
    ]).group_by { |record| record[foreign_key] }
  end
end

User.first.post_ids # => [1, 3, 9, 10, 15, 18, 19]
User.first.posts.each_limit_group(Comment, order: { created_at: :desc }, limit: 2)
__END__
{1=>
  [#<Comment:0x00007fb49d710418 id: 60, post_id: 1>,
   #<Comment:0x00007fb49d70bf80 id: 54, post_id: 1>],
 9=>
  [#<Comment:0x00007fb49d70b0d0 id: 58, post_id: 9>,
   #<Comment:0x00007fb49d70aba8 id: 40, post_id: 9>],
 10=>
  [#<Comment:0x00007fb49d70a6f8 id: 57, post_id: 10>,
   #<Comment:0x00007fb49d709d98 id: 50, post_id: 10>],
 15=>
  [#<Comment:0x00007fb49d709820 id: 51, post_id: 15>,
   #<Comment:0x00007fb49d7090f0 id: 34, post_id: 15>],
 3=>
  [#<Comment:0x00007fb49d70bb48 id: 31, post_id: 3>,
   #<Comment:0x00007fb49d70b620 id: 25, post_id: 3>]}
