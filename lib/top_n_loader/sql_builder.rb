module TopNLoader::SQLBuilder
  def self.condition_sql(klass, condition)
    condition_sql = where_condition_to_sql condition
    inheritance_column = klass.inheritance_column
    return condition_sql unless klass.has_attribute?(inheritance_column) && klass.base_class != klass
    sti_names = [klass, *klass.descendants].map(&:sti_name).compact
    sti_sql = where_condition_to_sql inheritance_column => sti_names
    [condition_sql, sti_sql].compact.join ' AND '
  end

  def self.top_n_child_sql(klass, relation, limit:, order_mode:, order_key:)
    reflection = klass.reflections[relation.to_s]
    parent_table = klass.table_name
    target_table = reflection.klass.table_name
    joins = klass.joins(relation).to_sql.match(/FROM.+/)[0]
    %(
      SELECT "#{target_table}".*, top_n_group_key
      #{joins}
      INNER JOIN
      (
        SELECT T.id as top_n_group_key,
        (
          SELECT "#{target_table}"."#{order_key}"
          #{joins}
          WHERE "#{parent_table}"."#{klass.primary_key}" = T."#{klass.primary_key}"
          ORDER BY "#{target_table}"."#{order_key}" #{order_mode.upcase}
          LIMIT 1 OFFSET #{limit}
        ) AS last_value
        FROM "#{parent_table}" as T where T."#{klass.primary_key}" in (?)
      ) T
      ON "#{parent_table}"."#{klass.primary_key}" = T.top_n_group_key
      AND (
        T.last_value IS NULL
        OR "#{target_table}"."#{order_key}" #{{asc: :<=, desc: :>=}[order_mode]} T.last_value
        OR "#{target_table}"."#{order_key}" is NULL
      )
    )
  end


  def self.top_n_group_sql(klass:, group_column:, group_keys:, condition:, limit:, order_mode:, order_key:)
    order_op = order_mode == :asc ? :<= : :>=
    group_key_table = value_table(:T, :group_key, group_keys)
    table_name = klass.table_name
    sql = condition_sql klass, condition
    join_cond = %("#{table_name}"."#{group_column}" = T.group_key)
    if group_keys.include? nil
      nil_join_cond = %(("#{table_name}"."#{group_column}" IS NULL AND T.group_key IS NULL))
      join_cond = %((#{join_cond} OR #{nil_join_cond}))
    end
    %(
      SELECT "#{table_name}".*, group_key as top_n_group_key
      FROM "#{table_name}"
      INNER JOIN
      (
        SELECT group_key,
        (
          SELECT "#{table_name}"."#{order_key}" FROM "#{table_name}"
          WHERE #{join_cond}
          #{"AND #{sql}" if sql}
          ORDER BY "#{table_name}"."#{order_key}" #{order_mode.to_s.upcase}
          LIMIT 1 OFFSET #{limit.to_i - 1}
        ) AS last_value
        FROM #{group_key_table}
      ) T
      ON #{join_cond}
      AND (
        T.last_value IS NULL
        OR "#{table_name}"."#{order_key}" #{order_op} T.last_value
        OR "#{table_name}"."#{order_key}" is NULL
      )
      #{"WHERE #{sql}" if sql}
    )
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
        %("#{key}" IS NULL)
      when Range
        if value.exclude_end?
          [%("#{key}" >= ? AND "#{key} < ?), value.begin, value.end]
        else
          [%("#{key}" BETWEEN ? AND ?), value.begin, value.end]
        end
      when Hash
        raise ArgumentError, '' unless value.keys == [:not]
        "NOT (#{kv_condition_to_sql(key, value[:not])})"
      when Enumerable
        array = value.to_a
        if array.include? nil
          [%(("#{key}" IS NULL OR "#{key}" IN (?))), array.reject(&:nil?)]
        else
          [%("#{key}" IN (?)), array]
        end
      else
        [%("#{key}" = ?), value]
      end
    end
    sanitize_sql_array sql_binds
  end

  def self.sanitize_sql_array(array)
    ActiveRecord::Base.send :sanitize_sql_array, array
  end
end
