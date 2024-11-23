# BskyProlificFollowers

A poorly coded daemon that listens to BlueSky Firehose events, and when one repo (A) follows another (B),
a check is made on how many accounts repo A follows. If that number is over 5,000, repo (user) A is
added to a list titled "Over5k"

## Installation

```bash
bundle install
```

Create a `creds.yml` like

```
id: your-bsky-username
pass: your-bsky-app-password
```

## Usage

```
./exec/bsky-prolific-followers
```

## Development

After checking out the repo, run `bin/setup` to install dependencies. Then, run `rake spec` to run the tests. You can also run `bin/console` for an interactive prompt that will allow you to experiment.

To install this gem onto your local machine, run `bundle exec rake install`. To release a new version, update the version number in `version.rb`, and then run `bundle exec rake release`, which will create a git tag for the version, push git commits and the created tag, and push the `.gem` file to [rubygems.org](https://rubygems.org).

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/jeffb4/bsky_prolific_followers.

## License

The gem is available as open source under the terms of the [MIT License](https://opensource.org/licenses/MIT).
