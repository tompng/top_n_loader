# syntax=docker/dockerfile:1
FROM ruby:3.1
RUN apt-get update -qq && apt-get install -y postgresql-client
WORKDIR /myapp
COPY . /myapp/
RUN bundle install
