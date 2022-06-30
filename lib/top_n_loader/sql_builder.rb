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
    limit = limit.to_i
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
    target_primary_key = "#{qt target_table}.#{q target_klass.primary_key}"
    top_n_key, top_n_alias, t_join_cond = limit == 1 ? [
      target_primary_key, :top_n_primary_key, "#{target_primary_key} = top_n_primary_key"
    ] : [
      target_order_key, :top_n_order_key, compare_cond(target_order_key, order_mode, includes_nil: nullable)
    ]
    order_columns = limit == 1 ? [target_order_key, target_primary_key].uniq : [target_order_key]
    order_cond = order_columns.map {|column| "#{column} #{order_mode.to_s.upcase}"}.join(', ')
    <<~SQL.squish
      SELECT #{qt target_table}.*, top_n_group_key
      #{join_sql}
      INNER JOIN
      (
        SELECT T.#{q klass.primary_key} AS top_n_group_key,
        (
          SELECT #{top_n_key}
          #{join_sql}
          WHERE #{parent_primary_key} = T.#{q klass.primary_key}
          ORDER BY #{order_cond}
          LIMIT 1#{" OFFSET #{limit - 1}" if limit != 1}
        ) AS #{top_n_alias}
        FROM #{qt parent_table} AS T WHERE T.#{q klass.primary_key} IN (?)
      ) T
      ON #{parent_primary_key} = T.top_n_group_key AND #{t_join_cond}
    SQL
  end

  def self.top_n_group_sql(klass:, group_column:, group_keys:, condition:, limit:, order_mode:, order_key:)
    limit = limit.to_i
    table_name = klass.table_name
    sql = condition_sql klass, condition
    group_key_nullable = group_keys.include?(nil) && nullable_column?(klass, group_column)
    order_key_nullable = nullable_column? klass, order_key
    group_key_table = value_table(:T, :top_n_group_key, group_keys)
    table_order_key = "#{qt table_name}.#{q order_key}"
    join_cond = equals_cond "#{qt table_name}.#{q group_column}", includes_nil: group_key_nullable
    table_primary_key = "#{qt table_name}.#{q klass.primary_key}"
    top_n_key, top_n_alias, t_join_cond = limit == 1 ? [
      table_primary_key, :top_n_primary_key, "#{table_primary_key} = top_n_primary_key"
    ] : [
      table_order_key, :top_n_order_key,
      "#{compare_cond table_order_key, order_mode, includes_nil: order_key_nullable}#{" WHERE #{sql}" if sql}"
    ]
    order_columns = limit == 1 ? [table_order_key, table_primary_key].uniq : [table_order_key]
    order_cond = order_columns.map {|column| "#{column} #{order_mode.to_s.upcase}"}.join(', ')
    <<~SQL.squish
      SELECT #{qt table_name}.*, top_n_group_key
      FROM #{qt table_name}
      INNER JOIN
      (
        SELECT top_n_group_key,
        (
          SELECT #{top_n_key} FROM #{qt table_name}
          WHERE #{join_cond}#{" AND #{sql}" if sql}
          ORDER BY #{order_cond}
          LIMIT 1#{" OFFSET #{limit - 1}" if limit != 1}
        ) AS #{top_n_alias}
        FROM #{group_key_table}
      ) T
      ON #{join_cond} AND #{t_join_cond}
    SQL
  end

  def self.equals_cond(column, includes_nil:, t_column: 'T.top_n_group_key')
    cond = "#{column} = #{t_column}"
    includes_nil ? "(#{cond} OR (#{column} IS NULL AND #{t_column} IS NULL))" : cond
  end

  def self.compare_cond(column, order_mode, includes_nil:, t_column: 'T.top_n_order_key')
    if order_mode == :desc
      "(#{column} >= #{t_column} OR #{t_column} IS NULL)"
    elsif includes_nil
      # t_column == nil if `result.size < limit` or `result[limit-1].order_column == nil`
      "(#{column} <= #{t_column} OR #{column} IS NULL OR #{t_column} IS NULL)"
    else
      "(#{column} <= #{t_column} OR #{t_column} IS NULL)"
    end
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
        "#{q key} IS NULL"
      when Range
        if value.exclude_end?
          ["#{q key} >= ? AND #{q key} < ?", value.begin, value.end]
        else
          ["#{q key} BETWEEN ? AND ?", value.begin, value.end]
        end
      when Hash
        raise ArgumentError, '' unless value.keys == [:not]
        "NOT (#{kv_condition_to_sql(key, value[:not])})"
      when Enumerable
        array = value.to_a
        if array.include? nil
          ["(#{q key} IS NULL OR #{q key} IN (?))", array.reject(&:nil?)]
        else
          ["#{q key} IN (?)", array]
        end
      else
        ["#{q key} = ?", value]
      end
    end
    sanitize_sql_array sql_binds
  end

  def self.sanitize_sql_array(array)
    ActiveRecord::Base.send :sanitize_sql_array, array
  end
end
