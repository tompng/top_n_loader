# TopNLoader

When you need top 5 sub-records for each record
```ruby
posts = Post.limit(10).to_a
```

Without TopNLoader: N+1 queries
```ruby
posts = Post.limit(10)
render json: posts.map do |post|
  {
    title: post.title,
    comments: post.comments.order(id: :desc).limit(5)
  }
end
```

With TopNLoader: Only 2 queries
```ruby
# One query here
posts = Post.limit(10).to_a
post_ids = posts.map(&:id)
# ANd just one query to load each comments(limit:5) for all posts
top5s = TopNLoader.load_associations Post, posts_ids, :comments, order: :desc, limit: 5
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

# will return the results below with a single query
records = ParentModel.find(ids).map do |record|
  [record.id, record.send(relation_name).order(order).take(limit)]
end.to_h
```

```ruby
TopNLoader.load_groups(YourModel, group_column, group_values, limit:, order: nil, condition: nil)
# limit: >=0
# order: :asc, :desc, {order_column: (:asc or :desc)}
# condition: 'name is null', ['name = ?', 'jack'], { age: (1..10), name: { not: 'jack' }}

# will return the results below with a single query
records = YourModel.where(condition).where(group_column => group_values).order(order)
records.group_by(&group_column).transform_values { |list| list.take(limit) }
```
