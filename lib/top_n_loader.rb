require 'top_n_loader/version'
require 'activerecord'
require 'top_n_loader/sql_builder'

module TopNLoader
  class << self
    def load(klass, column, keys, limit:, order: nil, condition: nil)
      order_key, order_mode = parse_order klass, order
      records = klass.find_by_sql(
        TopNRecords::SQLBuilder.top_n_sql(
          klass: klass,
          group_column: column,
          group_keys: keys,
          limit: limit,
          order_mode: order_mode,
          order_key: order_key,
          condition: condition
        )
      )
      format_result records, column, limit, order_mode, order_key
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

    def format_result(records, column, limit, order_mode, order_key)
      primary_key = klass.primary_key
      result = Hash.new { [] }.merge(records.group_by { |o| o[column] })
      result.transform_values do |grouped_records|
        existings, blanks = grouped_records.partition { |o| o[order_key] }
        existings.sort_by! { |o| [o[order_key], o[primary_key]] }
        blanks.sort_by! { |o| o[primary_key] }
        ordered = blanks + existings
        ordered.reverse! if order_mode == :desc
        ordered.take limit
      end
    end
  end
end
