module TopNLoader::SQLBuilder
  def self.condition_sql(klass, condition)
    condition_sql = where_condition_to_sql condition
    inheritance_column = klass.inheritance_column
    return condition_sql unless klass.has_attribute?(inheritance_column) && klass.base_class != klass
    sti_names = [klass, *klass.descendants].map(&:sti_name).compact
    sti_sql = where_condition_to_sql inheritance_column => sti_names
    [condition_sql, sti_sql].compact.join ' AND '
  end

  def self.top_n_association_sql(klass, relation, limit:, order_mode:, order_key:)
    parent_table = klass.table_name
    joins = klass.joins relation.to_sym
    target_table = joins.join_sources.last.left.name
    join_sql = joins.to_sql.match(/FROM.+/)[0]
    %(
      SELECT #{qt target_table}.*, top_n_group_key
      #{join_sql}
      INNER JOIN
      (
        SELECT T.#{q klass.primary_key} as top_n_group_key,
        (
          SELECT #{qt target_table}.#{q order_key}
          #{join_sql}
          WHERE #{qt parent_table}.#{q klass.primary_key} = T.#{q klass.primary_key}
          ORDER BY #{qt target_table}.#{q order_key} #{order_mode.upcase}
          LIMIT 1 OFFSET #{limit.to_i - 1}
        ) AS last_value
        FROM #{qt parent_table} as T where T.#{q klass.primary_key} in (?)
      ) T
      ON #{qt parent_table}.#{q klass.primary_key} = T.top_n_group_key
      AND (
        T.last_value IS NULL
        OR #{qt target_table}.#{q order_key} #{{ asc: :<=, desc: :>= }[order_mode]} T.last_value
        OR #{qt target_table}.#{q order_key} is NULL
      )
    )
  end


  def self.top_n_group_sql(klass:, group_column:, group_keys:, condition:, limit:, order_mode:, order_key:)
    order_op = order_mode == :asc ? :<= : :>=
    group_key_table = value_table(:T, :top_n_group_key, group_keys)
    table_name = klass.table_name
    sql = condition_sql klass, condition
    join_cond = %(#{qt table_name}.#{q group_column} = T.top_n_group_key)
    if group_keys.include? nil
      nil_join_cond = %((#{qt table_name}.#{q group_column} IS NULL AND T.top_n_group_key IS NULL))
      join_cond = %((#{join_cond} OR #{nil_join_cond}))
    end
    %(
      SELECT #{qt table_name}.*, top_n_group_key
      FROM #{qt table_name}
      INNER JOIN
      (
        SELECT top_n_group_key,
        (
          SELECT #{qt table_name}.#{q order_key} FROM #{qt table_name}
          WHERE #{join_cond}
          #{"AND #{sql}" if sql}
          ORDER BY #{qt table_name}.#{q order_key} #{order_mode.to_s.upcase}
          LIMIT 1 OFFSET #{limit.to_i - 1}
        ) AS last_value
        FROM #{group_key_table}
      ) T
      ON #{join_cond}
      AND (
        T.last_value IS NULL
        OR #{qt table_name}.#{q order_key} #{order_op} T.last_value
        OR #{qt table_name}.#{q order_key} is NULL
      )
      #{"WHERE #{sql}" if sql}
    )
  end

  def self.q(name)
    ActiveRecord::Base.connection.quote_column_name name
  end

  def self.qt(name)
    ActiveRecord::Base.connection.quote_table_name name
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
