# Voyager Helpers

A set of methods for retrieving data from Voyager.

[![Circle CI](https://circleci.com/gh/pulibrary/voyager_helpers.svg?style=svg)](https://circleci.com/gh/pulibrary/voyager_helpers)

## Installation

### Ubuntu
On __Ubuntu__ systems, do [this](https://help.ubuntu.com/community/Oracle%20Instant%20Client). __All of it.__

Add configuration for VGER In `$ORACLE_HOME/network/admin/tnsnames.ora` (ask DBA).

In `/etc/profile.d/oracle.sh` Append:

```bash
export ORACLE_LIB=$ORACLE_HOME/lib
export TNS_ADMIN=$ORACLE_HOME/network/admin
```

To the variables you added earlier.

### Mac
On __MacOSX__, follow the [ruby-oci8 instructions for setting up Oracle with Homebrew]
(http://www.rubydoc.info/gems/ruby-oci8/file/docs/install-on-osx.md).
You'll get the latest version of the client, which hopefully will be fine. The 11.2 client was known to work fine with 10.2 Oracle servers.

Download `tnsnames.txt` from the shared notes in lastpass. Name it `tnsnames.ora`, place it in `~/.tns/`, and set an environment variable:
```
export TNS_ADMIN=~/.tns
```

## Dev setup

Update the connection params in `lib/voyager_helpers/oracle_connection.rb` in
line with the readonly credentials (also stored in a shared note in lastpass).
Don't check this in to the repo.

To try it open an irb with `bundle exec irb`. Then do `require
'voyager_helpers'` and run whatever command you want.

If you get "client host name is not set" then you have to set your host name in
`/etc/hosts`. Here is [one
guide](http://johanlouwers.blogspot.com/2019/02/resolved-cxoracledatabaseerror-ora.html) for doing so.

## Configuration

Add the `voyager_helpers` and `ruby-oci8` gems to your application's Gemfile

```ruby
gem 'ruby-oci8'
gem 'voyager_helpers'
```

The gem needs to know the database username, password and database name. Put
this somewhere:

```ruby
VoyagerHelpers.configure do |config|
  config.du_user = 'foo'
  config.db_password = 'quux'
  config.db_name = 'VOYAGER'
end
```

(Like in an initializer if you're using Rails)

## Usage

Once everything is installed and configured, usage is pretty straightforward:

```ruby
record = VoyagerHelpers::Liberator.get_bib_record(4609321)
record.inspect
 => [#<MARC::Record:0x000000031781c8 @fields=[#<MARC::ControlField:0x00 ...
```

## Contributing

1. Fork it ( https://github.com/pulibrary/marc_liberation/fork )
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request
