# frozen_string_literal: true

require "concurrent"
require "date"
require "didkit"
require "json"
require "minisky"
require "skyfall"
require "sqlite3"
require "zlib"

module BskyProlificFollowers
  # BskyProlificFollowers::Listener - firehose listener
  class Listener
    def hydrate_db
      @cache_path = "cache.json.gz"
      return unless File.exist?(@cache_path)

      puts "Loading #{@cache_path}"

      did_profiles_local = {}
      Zlib::GzipReader.open(@cache_path) do |f|
        did_profiles_local = JSON.parse(f.read)
      end
      upsert_cache_stmt = @cache_db.prepare "INSERT INTO profiles VALUES (:did, :profile)"
      # hydrate did_profiles from JSON data, skipping nil (unretrieved) entries
      puts "Hydrating"
      did_profiles_local.each { |k, v| upsert_cache_stmt.execute("did" => k, "profile" => JSON.generate(v)) }
    end

    def init_db
      raise RuntimeError unless SQLite3.threadsafe?

      @cache_db = SQLite3::Database.new "cache.db"
      @cache_db.execute "CREATE TABLE IF NOT EXISTS profiles (did TEXT PRIMARY KEY, profile TEXT)"
      hydrate_db
    end

    def init_queues
      @did_query_queue = Queue.new
      @did_listadd_queue = Queue.new
      @did_schedule_queue = Queue.new
    end

    def init_blocklists
      @blocklists = Concurrent::Map.new
      @blocklists[:over5k] =
        { name: "Over5K", description: "Accounts that follow more than 5k accounts", threshold: 5000 }
      @blocklists[:over7k] =
        { name: "Over7K", description: "Accounts that follow more than 7k accounts", threshold: 7000 }
      @blocklists[:over10k] =
        { name: "Over10K", description: "Accounts that follow more than 10k accounts", threshold: 10_000 }
      @blocklists[:zws] =
        { name: "ZeroWidthSpace",
          description: "Accounts with descriptions containing suspicious zero width spaces and the like" }
      @blocklists[:mw] =
        { name: "MagaWords", description: "Profiles with MAGA terms in the description" }
      @blocklists[:hw] =
        { name: "HateWords", description: "Profiles with hateful terms in the description or account name" }
      @blocklists[:pw] =
        { name: "PornWords", description: "Profiles with porn terms in the description or account name" }
      @maga_words = load_words("maga_words.txt")
      @hate_words = load_words "hate_words.txt"
      @porn_words = load_words "porn_words.txt"
    end

    def initialize(num_profile_resolvers: 30, num_list_maintainers: 5, num_profile_schedulers: 1, verbose: false)
      @verbose = verbose
      init_db
      init_queues
      init_blocklists
      @did_profiles = Concurrent::Map.new
      @follows_limit = 5000
      @list_uris = Concurrent::Map.new
      @profile_schedulers = Concurrent::Array.new(num_profile_schedulers)
      @profile_resolvers = Concurrent::Array.new(num_profile_resolvers)
      @list_maintainers = Concurrent::Array.new(num_list_maintainers)
    end

    # load_words - Read a file given by filename, and return each line as a stripped string
    def load_words(filename)
      words = []
      return unless File.exist?(filename)

      File.open(filename) do |f|
        words = f.readlines
      end
      words.map(&:strip)
    end

    # add_user_to_list - add a given account DID to a list by URI
    def add_user_to_list(bsky, account_did, list_uri)
      bsky.post_request("com.atproto.repo.createRecord", {
                          repo: bsky.user.did,
                          collection: "app.bsky.graph.listitem",
                          record: { subject: account_did,
                                    list: list_uri,
                                    createdAt: DateTime.now.iso8601 }
                        })
    end

    def add_user_to_list_if_not_present(bsky, account_did, list_sym)
      if @blocklists[list_sym][:entries].include? account_did
        puts "@blocklists[#{list_sym}][:entries].include? #{account_did}" if @verbose
        return
      end
      puts "Adding #{account_did} to @blocklists[#{list_sym}][:entries] (len=#{@blocklists[list_sym][:entries].length})"

      add_user_to_list(bsky, account_did, @list_uris[list_sym])

      @blocklists[list_sym][:entries] << account_did
    end

    # check the follows on a profile and add to a list if appropriate
    def check_follows(bsky, profile)
      follows_count = profile["followsCount"]
      %i[over5k over7k over10k].each do |list_symbol|
        follow_limit = @blocklists[list_symbol][:threshold]
        if follows_count >= follow_limit
          puts "Adding #{profile["did"]} (#{follows_count} >= #{follows_limit})" if @verbose
          add_user_to_list_if_not_present(bsky, profile["did"], list_symbol)
        end
      end
    end

    # check the profile descript for zero width space (U+200b) and add to a list
    def check_zero_width_space(bsky, profile)
      # puts "Null ZWS check (#{account_did})"
      return unless profile.key? "description"

      description = profile["description"]
      # return unless description.include?("\u200b")
      return unless description =~ /[\u200B-\u200D]/
      return unless description =~ /\u200B/

      puts "Adding #{profile["did"]} contains zws in #{profile["description"]}" if @verbose
      add_user_to_list_if_not_present(bsky, profile["did"], :zws)
    end

    # match_dhd? does a profile description, handle, or displayName match an array of words?
    def match_dhd?(profile, words)
      description = profile["description"]
      handle = profile["handle"]
      display_name = profile["displayName"]
      return false unless words.any? do |w|
        description =~ /\W#{w}\W/i ||
        handle =~ /\W#{w}\W/i ||
        display_name =~ /\W#{w}\W/i
      end

      true
    end

    # check the profile description for presence of maga words and add to a list
    def check_maga_words(bsky, profile)
      return unless profile.key? "description"
      return unless match_dhd?(profile, @maga_words)

      puts "Adding #{profile["did"]} contains maga_words" if @verbose
      add_user_to_list_if_not_present(bsky, profile["did"], :mw)
    end

    # check the profile description for presence of hate words and add to a list
    def check_hate_words(bsky, profile)
      return unless profile.key? "description"
      return unless match_dhd?(profile, @hate_words)

      puts "Adding #{profile["did"]} contains hate_words" if @verbose
      add_user_to_list_if_not_present(bsky, profile["did"], :hw)
    end

    # check profile for presence of porn words and add to a list
    def check_porn_words(bsky, profile)
      return unless profile.key? "description"
      return unless match_dhd?(profile, @porn_words)

      puts "Adding #{profile["did"]} contains porn_words" if @verbose
      add_user_to_list_if_not_present(bsky, profile["did"], :pw)
    end

    # Get an account DID from the local cache
    def cache_get_profile(did)
      profile_json = @cache_db.execute("SELECT profile FROM profiles WHERE did=?", did)
      # puts "Got profile #{profile_json}"
      return nil unless profile_json[0]
      return nil if profile_json[0][0] == "null"

      JSON.parse(profile_json[0][0])
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
              profile = cache_get_profile(listadd_did)
              next unless profile

              check_follows(bsky, profile)
              check_zero_width_space(bsky, profile)
              check_maga_words(bsky,  profile)
              check_hate_words(bsky,  profile)
              check_porn_words(bsky,  profile)
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

    # cache_save_profile - upsert a profile to a given account DID in the local cache
    def cache_save_profile(did, profile)
      puts "Saving profile for #{did}" if @verbose
      raise "Attempted to upsert #{did} #{profile}" if profile == "null"

      @cache_db.execute("INSERT INTO profiles (did, profile) VALUES (?,?) " \
      "ON CONFLICT(did) DO UPDATE SET profile=excluded.profile",
                        [did, JSON.dump(profile)])
    end

    # cache_fresh?(did) - determine whether a cached profile DID is fresh enough (has been cached in the last hour)
    def cache_fresh?(did)
      ((DateTime.now - DateTime.iso8601(cache_get_profile(did)["cachedAt"])) * 86_400) < 3600
    end

    # get_profile(did) - query bsky using the public cached endpoint for a profile
    def get_profile(did)
      bsky = Minisky.new("public.api.bsky.app", nil)
      bsky.get_request(
        "app.bsky.actor.getProfile",
        { actor: did }
      )
    end

    # create_profile_resolvers - create threads that read from @did_query_queue and resolve profiles
    def create_profile_resolvers
      @profile_resolvers.map! do |thr|
        next thr unless thr.nil? || thr.status.nil?

        Thread.new do
          bsky = Minisky.new("public.api.bsky.app", nil)
          loop do
            lookup_did = @did_query_queue.pop
            # Recheck on whether we should skip profile fetch - it's possible a did was reentered into
            # resolver queue before it was fetched, so profile schedulers shouldn't necessarily be
            # trusted
            if cache_skip_profile_fetch?(lookup_did)
              puts "Resolver received cached DID #{lookup_did}" if @verbose
            else
              begin
                puts "Retrieving uncached DID profile #{lookup_did}" if @verbose
                profile = bsky.get_request(
                  "app.bsky.actor.getProfile",
                  { actor: lookup_did }
                )
                profile["cachedAt"] = DateTime.now.iso8601
                cache_save_profile(lookup_did, profile)
                # puts "(new) #{profile}"
                @did_listadd_queue.push(lookup_did)
              rescue Minisky::ClientErrorResponse => e
                puts e
              rescue Net::OpenTimeout => e
                puts(e.full_message)
                bsky = Minisky.new("public.api.bsky.app", nil)
                retry
              rescue Socket::ResolutionError
                puts "DNS resolution error"
                bsky = Minisky.new("public.api.bsky.app", nil)
                retry
              end
            end
          end
        end
      end
    end

    def cache_did_profile_exists?(did)
      profile_did = @cache_db.execute("SELECT profile FROM profiles WHERE did=?", did)
      return false if profile_did.nil?

      return false unless profile_did && profile_did[0]

      return false if profile_did[0][0] == "null"

      true
    end

    def create_profile_schedulers
      @profile_schedulers.map! do |thr|
        next thr unless thr.nil? || thr.status.nil?

        Thread.new do
          loop do
            firehose_did = @did_schedule_queue.pop
            if cache_did_profile_exists?(firehose_did)
              profile = cache_get_profile(firehose_did)
              if profile.nil?
                print "cache_profile_exists? but nil: #{firehose_did} "
                puts @cache_db.execute("SELECT profile FROM profiles WHERE did=?", firehose_did)
              elsif profile.key?("handle")
                puts "Received cached DID #{firehose_did} #{profile["handle"]}" if @verbose
              else
                puts "(ERROR) Recieved cached DID #{firehose_did} #{profile}"
              end
            else
              puts "Received uncached DID #{firehose_did}" if @verbose
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
      puts "Starting resolver cache debug"
      Thread.new do
        loop do
          sleep 5
          # if @did_query_queue.length > 0
          print "@did_schedule_queue.length = #{@did_schedule_queue.length} "
          print "@did_query_queue.length = #{@did_query_queue.length} "
          puts "@did_listadd_queue.length = #{@did_listadd_queue.length}"
          # end
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

    def read_list_entries(bsky, list_symbol)
      puts "Reading list entries for #{list_symbol}"
      list_entries = bsky.fetch_all("app.bsky.graph.getList",
                                    { list: @list_uris[list_symbol] },
                                    field: "items")
      list_members = Concurrent::Array.new
      list_entries.each do |v|
        list_members << v["subject"]["did"]
      end
      list_members
    end

    def load_lists
      bsky = Minisky.new("bsky.social", "creds.yml")
      bsky_public = Minisky.new("public.api.bsky.app", nil)
      @blocklists.each_key do |l|
        create_list_if_missing(bsky, l)
        @blocklists[l][:entries] = read_list_entries(bsky_public, l)
        puts "@blocklists[#{l}][:entries].length = #{@blocklists[l][:entries].length}"
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

    def clear_queues
      @did_listadd_queue.clear
      @did_query_queue.clear
      @did_schedule_queue.clear
    end

    def exit_threads
      (@profile_resolvers + @profile_schedulers + @list_maintainers).each(&:exit)
    end

    def queue_cache_rescan
      @cache_db.execute("SELECT did FROM profiles") do |row|
        @did_query_queue.push(row[0])
      end
    end

    # run the firehose listener
    def run
      puts "@hate_words = #{@hate_words}"
      load_lists
      queue_cache_rescan
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
        clear_queues
        exit_threads
      end
      @cache_db.close
    end

    # remove_rkey_from_lists - remove a given rkey from lists
    def remove_rkey_from_lists(bsky, rkey)
      bsky.post_request("com.atproto.repo.deleteRecord", {
                          repo: bsky.user.did,
                          collection: "app.bsky.graph.listitem",
                          rkey: rkey
                        })
    end

    # remove_user_from_list - remove a given account DID from a list by URI
    def remove_user_from_list(bsky, user_did, list_uri)
      bsky_public = Minisky.new("public.api.bsky.app", nil)
      entries = bsky_public.fetch_all("app.bsky.graph.getList",
                                      { list: list_uri },
                                      field: "items")

      unless entries.any? { |e| e["subject"]["did"] == user_did }
        puts "ERROR: user_did #{user_did} not found on list #{list_uri}"
        exit 1
      end

      print "Removing user #{user_did} from list #{list_uri}"
      entries.each do |entry|
        next unless entry["subject"]["did"] == user_did

        remove_rkey_from_lists(bsky, entry["uri"].split("/")[-1])
        break
      end
      puts " (complete)"
    end

    def run_remove_user_from_list(user:, list:, verbose:)
      bsky = Minisky.new("bsky.social", "creds.yml")
      account_lists = get_account_lists(bsky)
      list_uri = nil
      account_lists.each { |l| list_uri = l["uri"] if l["name"] == list }

      if list_uri.nil?
        puts "ERROR: no list found matching #{list}"
        exit 1
      end
      puts "list_uri = #{list_uri}" if verbose

      resolver = DIDKit::Resolver.new
      user_did = resolver.resolve_handle(user).did
      puts "user_did = #{user_did}" if verbose
      remove_user_from_list(bsky, user_did, list_uri)
    end
  end
end
