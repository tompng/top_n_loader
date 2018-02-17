# TopNLoader

各グループ毎に上位5件ずつレコードを取りたい時

こうやってN+1回クエリが出てしまうところを
```ruby
posts = Post.limit(10)
render json: posts.map do |post|
  {
    title: post.title,
    comments: post.comments.order(id: :desc).limit(5)
  }
end
```

1回のクエリでとってくれます
```ruby
posts = Post.limit(10)
top5s = TopNLoader.load_associations Post, posts.ids, :comments, order: :desc, limit: 5
render json: posts.map do |post|
  {
    title: post.title,
    comments: top5s[post.id]
  }
end
```

# Usage

```ruby
# Gemfile
gem 'top_n_loader', github: 'tompng/top_n_loader'
```

```ruby
TopNLoader.load_associations(ParentModel, ids, relation_name, limit:, order: nil)
# limit: >=0
# order: :asc, :desc, {order_column: (:asc or :desc)}

# 以下と同じ結果を返します(orderのフォーマットが若干違う)
records = ParentModel.find(ids).map do |record|
  [record.id, record.send(relation_name).order(order).take(limit)]
end.to_h
```

```ruby
TopNLoader.load_groups(YourModel, group_column, group_values, limit:, order: nil, condition: nil)
# limit: >=0
# order: :asc, :desc, {order_column: (:asc or :desc)}
# condition: 'name is null', ['name = ?', 'jack'], { age: (1..10), name: { not: 'jack' }}

# 以下と同じ結果を返します(conditionとorderのフォーマットが若干違う)
records = YourModel.where(condition).where(group_column => group_values).order(order)
records.group_by(&group_column).transform_values { |list| list.take(limit) }
```
