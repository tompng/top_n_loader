require 'simplecov'
SimpleCov.start 'test_frameworks'
$LOAD_PATH.unshift File.expand_path("../../lib", __FILE__)
require "top_n_loader"
require_relative './db'
DB.migrate
DB.seed
require "minitest/autorun"
