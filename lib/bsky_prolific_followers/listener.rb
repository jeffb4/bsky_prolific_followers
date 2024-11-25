# frozen_string_literal: true

require "concurrent"
require "date"
require "didkit"
require "json"
require "minisky"
require "skyfall"
require "zlib"

module BskyProlificFollowers
  # BskyProlificFollowers::Listener - firehose listener
  class Listener
    def initialize(num_profile_resolvers = 25, num_list_maintainers = 5, num_profile_schedulers = 1)
      @did_query_queue = Queue.new
      @did_listadd_queue = Queue.new
      @did_schedule_queue = Queue.new
      @did_profiles = Concurrent::Map.new
      @follows_limit = 5000
      @blocklists = Concurrent::Map.new
      @blocklists[:over5k] = { name: "Over5K", description: "Accounts that follow more than 5k accounts" }
      @blocklists[:zws] =
        { name: "ZeroWidthSpace",
          description: "Accounts with descriptions containing suspicious zero width spaces and the like" }
      @blocklists[:mw] =
        { name: "MagaWords", description: "Profiles with MAGA terms in the description" }
      @blocklists[:hw] =
        { name: "HateWords", description: "Profiles with hateful terms in the description or account name" }
      @list_uris = Concurrent::Map.new
      @profile_schedulers = Concurrent::Array.new(num_profile_resolvers)
      @profile_resolvers = Concurrent::Array.new(num_profile_resolvers)
      @list_maintainers = Concurrent::Array.new(num_list_maintainers)
      @cache_path = "cache.json.gz"
      @maga_words = load_words("maga_words.txt")
      @hate_words = load_words "hate_words.txt"
    end

    def load_words(filename)
      words = []
      return unless File.exist?(filename)

      File.open(filename) do |f|
        words = f.readlines
      end
      words.map(&:strip)
    end

    def add_user_to_list(bsky, account_did, list_uri)
      bsky.post_request("com.atproto.repo.createRecord", {
                          repo: bsky.user.did,
                          collection: "app.bsky.graph.listitem",
                          record: { subject: account_did,
                                    list: list_uri,
                                    createdAt: DateTime.now.iso8601 }
                        })
    end

    # check the follows on a profile and add to a list if appropriate
    def check_follows(bsky, account_did)
      follows_count = @did_profiles[account_did]["followsCount"]
      return if follows_count <= @follows_limit

      puts "Adding #{account_did} (#{follows_count} > #{@follows_limit})"
      add_user_to_list(bsky, account_did, @list_uris[:over5k])
    end

    # check the profile descript for zero width space (U+200b) and add to a list
    def check_zero_width_space(bsky, account_did)
      # puts "Null ZWS check (#{account_did})"
      return unless @did_profiles[account_did].key? "description"

      description = @did_profiles[account_did]["description"]
      # return unless description.include?("\u200b")
      return unless description =~ /[\u200B-\u200D]/
      return unless description =~ /\u200B/

      puts "Adding #{account_did} contains zws in #{@did_profiles[account_did]["description"]}"
      add_user_to_list(bsky, account_did, @list_uris[:zws])
    end

    # check the profile description for presence of maga words and add to a list
    def check_maga_words(bsky, account_did)
      return unless @did_profiles[account_did].key? "description"

      description = @did_profiles[account_did]["description"]
      handle = @did_profiles[account_did]["handle"]
      display_name = @did_profiles[account_did]["displayName"]
      return unless @maga_words.any? do |w|
        description =~ /#{w}\W/i ||
        handle =~ /#{w}\W/i ||
        display_name =~ /#{w}\W/i
      end

      puts "Adding #{account_did} contains maga_words in #{description}"
      add_user_to_list(bsky, account_did, @list_uris[:mw])
    end

    # check the profile description for presence of maga words and add to a list
    def check_hate_words(bsky, account_did)
      return unless @did_profiles[account_did].key? "description"

      description = @did_profiles[account_did]["description"]
      handle = @did_profiles[account_did]["handle"]
      display_name = @did_profiles[account_did]["displayName"]
      return unless @hate_words.any? do |w|
        description =~ /#{w}\W/i ||
        handle =~ /#{w}\W/i ||
        display_name =~ /#{w}\W/i
      end

      puts "Adding #{account_did} contains hate_words in #{handle} / #{display_name} / #{description}"
      add_user_to_list(bsky, account_did, @list_uris[:hw])
    end

    # Create list maintainer threads when needed
    def create_list_maintainers
      @list_maintainers.map! do |thr|
        # if the thread entry is nil (never created) or the status is nil (crashed),
        # create a new thread for this entry
        next thr unless thr.nil? || thr.status.nil?

        Thread.new do
          bsky = Minisky.new("bsky.social", "creds.yml")
          loop do
            listadd_did = @did_listadd_queue.pop
            begin
              check_follows(bsky, listadd_did)
              check_zero_width_space(bsky, listadd_did)
              check_maga_words(bsky, listadd_did)
              check_hate_words(bsky, listadd_did)
            rescue Minisky::ExpiredTokenError => e
              puts(e.full_message)
              bsky = Minisky.new("bsky.social", "creds.yml")
              retry
            rescue Minisky::ServerErrorResponse => e
              puts(e.full_message)
              retry
            end
          end
        end
      end
    end

    def create_profile_resolvers
      @profile_resolvers.map! do |thr|
        next thr unless thr.nil? || thr.status.nil?

        Thread.new do
          bsky = Minisky.new("public.api.bsky.app", nil)
          loop do
            lookup_did = @did_query_queue.pop
            if @did_profiles.key?(lookup_did) && !@did_profiles[lookup_did].nil?
              puts "Resolver received cached DID #{lookup_did}"
            else
              begin
                # puts "Retrieving uncached DID profile #{lookup_did}"
                profile = bsky.get_request(
                  "app.bsky.actor.getProfile",
                  {
                    actor: lookup_did
                  }
                )
                profile["cachedAt"] = DateTime.now.iso8601
                @did_profiles[lookup_did] = profile
                # puts "(new) #{profile}"
                @did_listadd_queue.push(lookup_did)
              rescue Minisky::ClientErrorResponse => e
                puts e
              rescue Net::OpenTimeout => e
                puts(e.full_message)
                bsky = Minisky.new("public.api.bsky.app", nil)
                retry
              end
            end
          end
        end
      end
    end

    def create_profile_schedulers
      @profile_schedulers.map! do |thr|
        next thr unless thr.nil? || thr.status.nil?

        Thread.new do
          loop do
            firehose_did = @did_schedule_queue.pop
            if @did_profiles.key?(firehose_did)
              # puts "Received cached DID #{firehose_did}"
            else
              # puts "Received uncached DID #{firehose_did}"
              @did_profiles[firehose_did] = nil
              @did_query_queue.push(firehose_did)
            end
          end
        end
      end
    end

    def maintainer_helpers
      create_profile_schedulers
      create_list_maintainers
      create_profile_resolvers
    end

    def create_maintainer_helpers_timer
      task = Concurrent::TimerTask.new(execution_interval: 5, run_now: true) { maintainer_helpers }
      task.execute
    end

    def debug_resolver_cache
      Thread.new do
        loop do
          sleep 5
          if @did_query_queue.length > 200
            print "@did_query_queue.length = #{@did_query_queue.length} "
            puts "@did_listadd_queue.length = #{@did_listadd_queue.length}"
          end
          did_profiles_local = {}
          @did_profiles.each { |k, v| did_profiles_local[k] = v.dup }
          # puts did_profiles_local
        end
      end
    end

    def create_list(bsky, list_name, list_description)
      puts "Creating a list named #{list_name}"
      bsky.post_request("com.atproto.repo.createRecord",
                        {
                          repo: bsky.user.did,
                          collection: "app.bsky.graph.list",
                          record: {
                            "$type": "app.bsky.graph.list",
                            purpose: "app.bsky.graph.defs#modlist",
                            name: list_name,
                            description: list_description,
                            createdAt: DateTime.now.iso8601
                          }
                        })["uri"]
    end

    def get_account_lists(bsky)
      bsky.get_request("app.bsky.graph.getLists", { actor: bsky.user.did })["lists"]
    end

    def create_list_if_missing(bsky, list_id)
      list_name = @blocklists[list_id][:name]

      matched_list = get_account_lists(bsky).find { |list_entry| list_entry["name"] == list_name }
      if matched_list
        puts "Found a list named #{list_name}"
        @list_uris[list_id] = matched_list["uri"]
      else
        @list_uris[list_id] = create_list(bsky, list_name, @blocklists[list_id][:description])
      end
      puts "list_uri is #{@list_uris[list_id]}"
    end

    def create_lists
      bsky = Minisky.new("bsky.social", "creds.yml")
      @blocklists.each_key { |l| create_list_if_missing(bsky, l) }
    end

    def dump_cache
      puts "Dumping cache to #{@cache_path}"
      did_profiles_local = {}
      @did_profiles.each { |k, v| did_profiles_local[k] = v.dup }
      Zlib::GzipWriter.open(@cache_path) do |f|
        f << did_profiles_local.to_json
      end
    end

    def load_cache
      return unless File.exist?(@cache_path)

      puts "Loading #{@cache_path}"

      did_profiles_local = {}
      Zlib::GzipReader.open(@cache_path) do |f|
        did_profiles_local = JSON.parse(f.read)
      end
      # hydrate did_profiles from JSON data, skipping nil (unretrieved) entries
      did_profiles_local.each { |k, v| @did_profiles[k] = v.dup unless v.nil? }
      puts "Loaded #{@did_profiles.keys.length} profiles"
    end

    # run the firehose listener
    def run
      puts "@hate_words = #{@hate_words}"
      create_lists
      load_cache
      @did_profiles.each_key { |k| @did_listadd_queue.push(k) }
      create_maintainer_helpers_timer
      debug_resolver_cache

      sky = Skyfall::Firehose.new("bsky.network", :subscribe_repos)

      # Listen on the firehose for follow actions
      sky.on_message do |msg|
        # puts "Saw #{msg.did} on firehose"
        @did_schedule_queue.push(msg.did)
      end

      # lifecycle events
      sky.on_connecting { |url| puts "Connecting to #{url}..." }
      sky.on_connect { puts "Connected" }
      sky.on_disconnect { puts "Disconnected" }
      sky.on_reconnect { puts "Connection lost, trying to reconnect..." }
      sky.on_timeout { puts "Connection stalled, triggering a reconnect..." }

      # handling errors (there's a default error handler that does exactly this)
      sky.on_error { |e| puts "ERROR: #{e}" }

      begin
        sky.connect
      rescue Interrupt
        dump_cache
      end
    end
  end
end
