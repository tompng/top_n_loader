# TopNLoader

各グループ毎に上位5件ずつレコードを取りたい時

こうやってN+1回クエリが出てしまうところを
```ruby
posts = Post.limit(10)
render json: posts.map do |post|
  {
    title: post.title,
    comments: post.comments.order(created_at: :desc).limit(5)
  }
end
```

1回のクエリでとってくれます
```ruby
posts = Post.limit(10)
top5s = TopNLoader.load Comment, :post_id, posts.ids, order: { created_at: :desc }, limit: 5
render json: posts.map do |post|
  {
    title: post.title,
    comments: top5s[post.id]
  }
end
```

```ruby
gem 'top_n_loader', github: 'tompng/top_n_loader'
TopNLoader.load(YourModel, group_column, group_values, limit:, order: nil, condition: nil)
# limit: >=0
# order: :asc, :desc, {order_column: (:asc or :desc)}
# condition: 'name is null', ['name = ?', 'jack'], { age: (1..10), name: { not: 'jack' }}
```
