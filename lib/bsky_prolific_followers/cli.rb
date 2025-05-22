# frozen_string_literal: true

require "bsky_prolific_followers/listener"
require "bsky_prolific_followers/modlist"
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
           delete-list:      delete a modlist
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
        end,
        "delete-list" => OptionParser.new do |opts|
          opts.banner = "Usage: delete-list [options]"
          opts.on("-lLIST", "--list=LIST", "list to delete") do |n|
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

      case command
      when "run"
        listener = BskyProlificFollowers::Listener.new(verbose: options[:verbose])
        listener.run(expire: options[:expire])
      when "remove-user"
        BskyProlificFollowers::Listener.run_remove_user_from_list(user: options[:user], list: options[:list],
                                                                  verbose: options[:verbose])
      when "delete-list"
        BskyProlificFollowers::Modlist.run_delete_list(list: options[:list])
      else
        return STATUS_ERROR
      end
      STATUS_SUCCESS
    end

    def exit?
      @exit
    end
  end
end
