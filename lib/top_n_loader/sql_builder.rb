module TopNLoader::SQLBuilder
  def self.condition_sql(klass, condition)
    condition_sql = where_condition_to_sql condition
    inheritance_column = klass.inheritance_column
    return condition_sql unless klass.has_attribute?(inheritance_column) && klass.base_class != klass
    sti_names = [klass, *klass.descendants].map(&:sti_name).compact
    sti_sql = where_condition_to_sql inheritance_column => sti_names
    [condition_sql, sti_sql].compact.join ' AND '
  end

  def self.top_n_association_sql(klass, target_klass, relation, limit:, order_mode:, order_key:)
    parent_table = klass.table_name
    joins = klass.joins relation.to_sym
    target_table = target_klass.table_name
    nullable = nullable_column? target_klass, order_key
    if target_table == klass.table_name
      target_table = "#{joins.joins_values.first.to_s.pluralize}_#{target_table}"
    end
    join_sql = joins.to_sql.match(/FROM.+/)[0]
    parent_primary_key = "#{qt parent_table}.#{q klass.primary_key}"
    target_order_key = "#{qt target_table}.#{q order_key}"
    return top_aggregate_sql target_table, order_key, parent_primary_key, from_sql: join_sql, order_mode: order_mode if limit == 1 && !nullable

    <<~SQL.squish
      SELECT #{qt target_table}.*, top_n_group_key
      #{join_sql}
      INNER JOIN
      (
        SELECT T.#{q klass.primary_key} AS top_n_group_key,
        (
          SELECT #{target_order_key}
          #{join_sql}
          WHERE #{parent_primary_key} = T.#{q klass.primary_key}
          ORDER BY #{target_order_key} #{order_mode.upcase}
          LIMIT 1 OFFSET #{limit.to_i - 1}
        ) AS last_value_of_key
        FROM #{qt parent_table} AS T WHERE T.#{q klass.primary_key} IN (?)
      ) T
      ON #{parent_primary_key} = T.top_n_group_key AND #{compare_cond target_order_key, order_mode, includes_nil: nullable}
    SQL
  end

  def self.top_n_group_sql(klass:, group_column:, group_keys:, condition:, limit:, order_mode:, order_key:)
    table_name = klass.table_name
    sql = condition_sql klass, condition
    group_key_nullable = group_keys.include?(nil) && nullable_column?(klass, group_column)
    order_key_nullable = nullable_column? klass, order_key
    if limit == 1 && !order_key_nullable
      generated_sql = top_aggregate_sql(
        table_name,
        order_key,
        "#{qt table_name}.#{q group_column}",
        from_sql: "FROM #{qt table_name}",
        condition_sql: sql,
        includes_nil: group_key_nullable,
        order_mode: order_mode
      )
      return [generated_sql, group_keys - [nil]]
    end
    group_key_table = value_table(:T, :top_n_group_key, group_keys)
    table_order_key = "#{qt table_name}.#{q order_key}"
    join_cond = equals_cond "#{qt table_name}.#{q group_column}", includes_nil: group_key_nullable
    <<~SQL.squish
      SELECT #{qt table_name}.*, top_n_group_key
      FROM #{qt table_name}
      INNER JOIN
      (
        SELECT top_n_group_key,
        (
          SELECT #{table_order_key} FROM #{qt table_name}
          WHERE #{join_cond}
          #{"AND #{sql}" if sql}
          ORDER BY #{table_order_key} #{order_mode.to_s.upcase}
          LIMIT 1 OFFSET #{limit.to_i - 1}
        ) AS last_value_of_key
        FROM #{group_key_table}
      ) T
      ON #{join_cond}
      AND #{compare_cond table_order_key, order_mode, includes_nil: order_key_nullable}
      #{"WHERE #{sql}" if sql}
    SQL
  end

  def self.equals_cond(column, includes_nil:, t_column: 'T.top_n_group_key')
    cond = "#{column} = #{t_column}"
    includes_nil ? "(#{cond} OR (#{column} IS NULL AND #{t_column} IS NULL))" : cond
  end

  def self.compare_cond(column, order_mode, includes_nil:, t_column: 'T.last_value_of_key')
    if order_mode == :desc
      "(#{column} >= #{t_column} OR #{t_column} IS NULL)"
    elsif includes_nil
      # t_column == nil if `result.size < limit` or `result[limit-1].order_column == nil`
      "(#{column} <= #{t_column} OR #{column} IS NULL OR #{t_column} IS NULL)"
    else
      "(#{column} <= #{t_column} OR #{t_column} IS NULL)"
    end
  end

  def self.top_aggregate_sql(target_table, order_key, group_table_column, from_sql:, order_mode:, condition_sql: nil, includes_nil: false)
    target_order_key = "#{qt target_table}.#{q order_key}"
    order_func = order_mode == :asc ? :MIN : :MAX
    <<~SQL.squish
      SELECT #{qt target_table}.*, top_n_group_key
      #{from_sql}
      INNER JOIN
      (
        SELECT #{group_table_column} AS top_n_group_key, #{order_func}(#{target_order_key}) AS top_value_of_key
        #{from_sql}
        WHERE (#{group_table_column} IN (?)#{" OR #{group_table_column} IS NULL" if includes_nil})
        #{"AND #{condition_sql}" if condition_sql}
        GROUP BY #{group_table_column}
      ) T
      ON #{equals_cond group_table_column, includes_nil: includes_nil}
      AND #{target_order_key} = T.top_value_of_key
      #{"AND #{condition_sql}" if condition_sql}
    SQL
  end

  def self.q(name)
    ActiveRecord::Base.connection.quote_column_name name
  end

  def self.qt(name)
    ActiveRecord::Base.connection.quote_table_name name
  end

  def self.nullable_column?(klass, column)
    klass.column_for_attribute(column).null
  end

  def self.value_table(table, column, values)
    if ActiveRecord::Base.connection.adapter_name == 'PostgreSQL'
      values_value_table(table, column, values)
    else
      union_value_table(table, column, values)
    end
  end

  def self.union_value_table(table, column, values)
    sanitize_sql_array [
      "(SELECT ? AS #{column}#{' UNION SELECT ?' * (values.size - 1)}) AS #{table}",
      *values
    ]
  end

  def self.values_value_table(table, column, values)
    sanitize_sql_array [
      "(VALUES #{(['(?)'] * values.size).join(',')}) AS #{table} (#{column})",
      *values
    ]
  end

  def self.where_condition_to_sql(condition)
    case condition
    when String
      condition
    when Array
      sanitize_sql_array condition
    when Hash
      condition.map { |k, v| kv_condition_to_sql k, v }.join ' AND '
    end
  end

  def self.kv_condition_to_sql(key, value)
    return "NOT (#{where_condition_to_sql(value)})" if key == :not
    sql_binds = begin
      case value
      when NilClass
        %(#{q key} IS NULL)
      when Range
        if value.exclude_end?
          [%(#{q key} >= ? AND #{q key} < ?), value.begin, value.end]
        else
          [%(#{q key} BETWEEN ? AND ?), value.begin, value.end]
        end
      when Hash
        raise ArgumentError, '' unless value.keys == [:not]
        "NOT (#{kv_condition_to_sql(key, value[:not])})"
      when Enumerable
        array = value.to_a
        if array.include? nil
          [%((#{q key} IS NULL OR #{q key} IN (?))), array.reject(&:nil?)]
        else
          [%(#{q key} IN (?)), array]
        end
      else
        [%(#{q key} = ?), value]
      end
    end
    sanitize_sql_array sql_binds
  end

  def self.sanitize_sql_array(array)
    ActiveRecord::Base.send :sanitize_sql_array, array
  end
end
