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
  spec.license       = "MIT"

  # Prevent pushing this gem to RubyGems.org. To allow pushes either set the 'allowed_push_host'
  # to allow pushing to a single host or delete this section to allow pushing to any host.
  if spec.respond_to?(:metadata)
    spec.metadata["allowed_push_host"] = "TODO: Set to 'http://mygemserver.com'"
  else
    raise "RubyGems 2.0 or newer is required to protect against " \
      "public gem pushes."
  end

  spec.files         = `git ls-files -z`.split("\x0").reject do |f|
    f.match(%r{^(test|spec|features)/})
  end
  spec.bindir        = "exe"
  spec.executables   = spec.files.grep(%r{^exe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  spec.add_dependency "activerecord"

  %w[bundler rake minitest sqlite3 pry].each do |gem_name|
    spec.add_development_dependency gem_name
  end
end
