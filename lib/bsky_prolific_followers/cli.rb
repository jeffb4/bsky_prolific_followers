# frozen_string_literal: true

require "bsky_prolific_followers/listener"
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
      listener = BskyProlificFollowers::Listener.new
      listener.run
      STATUS_SUCCESS
    end

    def exit?
      @exit
    end
  end
end
