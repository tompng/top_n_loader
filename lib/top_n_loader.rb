require 'top_n_loader/version'
require 'active_record'
require 'top_n_loader/sql_builder'

module TopNLoader
  class << self
    def load_childs(klass, ids, relation, limit:, order: nil)
      child_class = klass.reflections[relation.to_s].klass
      order_key, order_mode = parse_order child_class, order
      order_option = { limit: limit, order_mode: order_mode, order_key: order_key }
      sql = SQLBuilder.top_n_child_sql klass, relation, order_option
      records = child_class.find_by_sql([sql, ids])
      format_result(records, klass: child_class, **order_option)
    end

    def load_groups(klass, column, keys, limit:, order: nil, condition: nil)
      raise ArgumentError, 'negative limit' if limit < 0
      return Hash.new { [] } if keys.empty? || limit.zero?
      order_key, order_mode = parse_order klass, order
      options = {
        klass: klass,
        group_column: column,
        limit: limit,
        order_mode: order_mode,
        order_key: order_key
      }
      records = klass.find_by_sql(
        SQLBuilder.top_n_group_sql(
          group_keys: keys,
          condition: condition,
          **options
        )
      )
      format_result records, options
    end

    private

    def parse_order(klass, order)
      key, mode = begin
        case order
        when Hash
          raise ArgumentError, 'invalid order' unless order.size == 1
          order.first
        when Symbol
          [klass.primary_key, order]
        when NilClass
          [klass.primary_key, :asc]
        end
      end
      raise ArgumentError, 'invalid order' unless %i[asc desc].include? mode
      [key, mode]
    end

    def format_result(records, klass:, group_column: nil, limit:, order_mode:, order_key:)
      primary_key = klass.primary_key
      type = klass.attribute_types[group_column.to_s] if group_column
      result = records.group_by do |record|
        key = record.top_n_group_key
        type ? type.cast(key) : key unless key.nil?
      end
      result.transform_values! do |grouped_records|
        existings, blanks = grouped_records.partition { |o| o[order_key] }
        existings.sort_by! { |o| [o[order_key], o[primary_key]] }
        blanks.sort_by! { |o| o[primary_key] }
        ordered = blanks + existings
        ordered.reverse! if order_mode == :desc
        ordered.take limit
      end
      Hash.new { [] }.update result
    end
  end
end
