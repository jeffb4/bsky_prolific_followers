# frozen_string_literal: true

require "bsky_prolific_followers/listener"
require "optparse"
# top-level module for utility
module BskyProlificFollowers
  # Command line handler for utility.
  class CLI
    STATUS_SUCCESS = 0
    STATUS_ERROR = 2
    def initialize
      @exit = false
    end

    def run(argv = ARGV)
      puts("in BskyProlificFollowers::CLI::run #{argv}")
      options = { verbose: false, expire: true }

      subtext = <<~HELP
        Commonly used command are:
           run :             run the bot
           remove-user :     remove a user from a list
        See 'opt.rb COMMAND --help' for more information on a specific command.
      HELP

      global = OptionParser.new do |opts|
        opts.banner = "Usage: bsky-prolific-followers [options] [subcommand [options]]"
        opts.on("-v", "--[no-]verbose", "Run verbosely") do |v|
          options[:verbose] = v
        end
        opts.separator ""
        opts.separator subtext
      end

      subcommands = {
        "run" => OptionParser.new do |opts|
          opts.banner = "Usage: run [options]"
          opts.on("-c", "--[no-]cache", "use cache file") do |v|
            options[:force] = v
          end
          opts.on("-x", "--[no-]expire-cache", "ignore cache entry age") do |v|
            options[:expire] = !v
          end
        end,
        "remove-user" => OptionParser.new do |opts|
          opts.banner = "Usage: remove-user [options]"
          opts.on("-uUSER", "--user=USER", "user to remove") do |n|
            options[:user] = n
          end
          opts.on("-lLIST", "--list=LIST", "list to remove user from") do |n|
            options[:list] = n
          end
        end
      }

      global.order! argv
      command = argv.shift
      subcommands[command].order! argv

      puts "Command: #{command} "
      p options
      puts "ARGV:"
      p argv

      listener = BskyProlificFollowers::Listener.new(verbose: options[:verbose])
      if command == "run"
        listener.run(expire: options[:expire])
      else
        listener.run_remove_user_from_list(user: options[:user], list: options[:list], verbose: options[:verbose])
      end
      STATUS_SUCCESS
    end

    def exit?
      @exit
    end
  end
end
