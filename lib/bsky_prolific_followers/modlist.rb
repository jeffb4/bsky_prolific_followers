# frozen_string_literal: true

require "didkit"
require "minisky"

module BskyProlificFollowers
  # BskyProlificFollowers::Modlist - modlist operations
  class Modlist
    def self.delete_list(list)
      bsky = Minisky.new("bsky.social", "creds.yml")
      bsky.post_request("com.atproto.repo.deleteRecord", {
                          repo: bsky.user.did,
                          collection: "app.bsky.graph.list",
                          rkey: list.split("/")[-1]
                        })
    end

    def self.run_delete_list(list:)
      delete_list(list)
    end
  end
end
