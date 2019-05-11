lib = File.expand_path("../lib", __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require "top_n_loader/version"

Gem::Specification.new do |spec|
  spec.name          = "top_n_loader"
  spec.version       = TopNLoader::VERSION
  spec.authors       = ["tompng"]
  spec.email         = ["tomoyapenguin@gmail.com"]

  spec.summary       = %q{load top n records for each group}
  spec.description   = %q{load top n records for each group}
  spec.homepage      = "https://github.com/tompng/#{spec.name}"
  spec.license       = "MIT"

  spec.files         = `git ls-files -z`.split("\x0").reject do |f|
    f.match(%r{^(test|spec|features)/})
  end
  spec.bindir        = "exe"
  spec.executables   = spec.files.grep(%r{^exe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  spec.add_dependency "activerecord"

  %w[bundler rake minitest sqlite3 pry simplecov].each do |gem_name|
    spec.add_development_dependency gem_name
  end
end
