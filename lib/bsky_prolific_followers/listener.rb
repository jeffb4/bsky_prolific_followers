# frozen_string_literal: true

require "concurrent"
require "date"
require "didkit"
require "minisky"
require "skyfall"

module BskyProlificFollowers
  # BskyProlificFollowers::Listener - firehose listener
  class Listener
    def initialize
      @did_query_queue = Queue.new
      @did_listadd_queue = Queue.new
      @did_resolution = Concurrent::Hash.new
      @did_follows = Concurrent::Hash.new
      @follows_limit = 5000
      @list_name = "Over5K"
      @list_uri = ""
    end

    def spawn_resolver(num_threads = 1)
      num_threads.times.map do
        Thread.new do
          resolver = DIDKit::Resolver.new
          loop do
            lookup_did = @did_query_queue.pop
            print "Received DID #{lookup_did}: "
            if @did_resolution.value?(lookup_did)
              puts "(cached) #{@did_resolution[lookup_did]}"
            else
              @did_resolution[lookup_did] = resolver.get_validated_handle(lookup_did)
              puts @did_resolution[lookup_did]
            end
          end
        end
      end
    end

    def check_list_adder
      return if @list_adder.any? { |thr| !thr.status.nil? }

      puts "All list_adder threads are nil"

      @list_adder = spawn_list_adder(5)
    end

    def spawn_list_adder(num_threads = 1)
      num_threads.times.map do
        Thread.new do
          bsky = Minisky.new("bsky.social", "creds.yml")
          loop do
            listadd_did = @did_listadd_queue.pop
            next if @did_follows[listadd_did] <= @follows_limit

            puts "Adding #{listadd_did} (#{@did_follows[listadd_did]} > #{@follows_limit})"
            bsky.post_request("com.atproto.repo.createRecord", {
                                repo: bsky.user.did,
                                collection: "app.bsky.graph.listitem",
                                record: {
                                  subject: listadd_did,
                                  list: @list_uri,
                                  createdAt: DateTime.now.iso8601
                                }
                              })
          end
        end
      end
    end

    def check_follows_helper
      return if @follows_helper.any? { |thr| !thr.status.nil? }

      puts "All follows_helper threads are nil"

      @follows_helper = spawn_follows_helper(20)
    end

    def spawn_follows_helper(num_threads = 1)
      num_threads.times.map do
        Thread.new do
          bsky = Minisky.new("public.api.bsky.app", nil)
          loop do
            lookup_did = @did_query_queue.pop
            print "Received DID #{lookup_did}: "
            if @did_follows.value?(lookup_did)
              puts "(cached) #{@did_follows[lookup_did]}"
            else
              follows_count = bsky.get_request(
                "app.bsky.actor.getProfile",
                {
                  actor: lookup_did
                }
              )["followsCount"]
              @did_follows[lookup_did] = follows_count
              puts "(new) #{@did_follows[lookup_did]}"
              @did_listadd_queue.push(lookup_did)
            end
          end
        end
      end
    end

    def debug_resolver_cache
      Thread.new do
        sleep 30
        puts @did_resolution
      end
    end

    def create_list
      bsky = Minisky.new("bsky.social", "creds.yml")
      # Search account lists to ensure it doesn't exist yet
      account_lists = bsky.get_request("app.bsky.graph.getLists",
                                       {
                                         actor: bsky.user.did
                                       })["lists"]
      matched_list = account_lists.find { |list_entry| list_entry["name"] == @list_name }
      if matched_list
        puts "Found a list named #{@list_name}"
        @list_uri = matched_list["uri"]
        puts "list_uri is #{@list_uri}"
        return
      end
      # create a new list
      puts "Creating a list named #{@list_name}"
      @list_uri = bsky.post_request("com.atproto.repo.createRecord",
                                    {
                                      repo: bsky.user.did,
                                      collection: "app.bsky.graph.list",
                                      record: {
                                        "$type": "app.bsky.graph.list",
                                        purpose: "app.bsky.graph.defs#modlist",
                                        name: "Over5K",
                                        description: "Accounts that follow more than 5k accounts",
                                        createdAt: DateTime.now.iso8601
                                      }
                                    })["uri"]
    end

    # run the firehose listener
    def run
      # did_resolver_thread = spawn_resolver(20)
      create_list
      @follows_helper = spawn_follows_helper(20)
      @list_adder = spawn_list_adder(5)
      # debug_resolver_cache
      sky = Skyfall::Firehose.new("bsky.network", :subscribe_repos)

      # Listen on the firehose for follow actions
      sky.on_message do |msg|
        if msg.type == :commit &&
           msg.operations &&
           msg.operations[0] &&
           msg.operations[0].path =~ %r{^app.bsky.graph.follow/}
          @did_query_queue.push(msg.did)
        else
          check_follows_helper
          check_list_adder
        end
      end

      # lifecycle events
      sky.on_connecting { |url| puts "Connecting to #{url}..." }
      sky.on_connect { puts "Connected" }
      sky.on_disconnect { puts "Disconnected" }
      sky.on_reconnect { puts "Connection lost, trying to reconnect..." }
      sky.on_timeout { puts "Connection stalled, triggering a reconnect..." }

      # handling errors (there's a default error handler that does exactly this)
      sky.on_error { |e| puts "ERROR: #{e}" }

      sky.connect
    end
  end
end
