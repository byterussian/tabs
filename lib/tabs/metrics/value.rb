module Tabs
  module Metrics
    class Value
      include Storage
      include Helpers

      attr_reader :key

      def initialize(key)
        @key = key
      end

      def record(value, timestamp=Time.now)
        timestamp.utc
        Tabs::Resolution.all.each do |resolution|
          store_key = storage_key(resolution, timestamp)
          update_values(store_key, value)
          Tabs::Resolution.expire(resolution, store_key, timestamp)
        end
        true
      end

      def delete(value, timestamp=Time.now)
        timestamp.utc
        Tabs::Resolution.all.each do |resolution|
          store_key = storage_key(resolution, timestamp)
          delete_values(store_key, value)
          Tabs::Resolution.expire(resolution, store_key, timestamp)
        end
        true
      end

      def stats(period, resolution)
        timestamps = timestamp_range period, resolution
        keys = timestamps.map do |timestamp|
          storage_key(resolution, timestamp)
        end

        values = mget(*keys).map do |v|
          value = v.nil? ? default_value(0) : JSON.parse(v)
          value["timestamp"] = timestamps.shift
          value.with_indifferent_access
        end

        Stats.new(period, resolution, values)
      end

      def drop!
        del_by_prefix("stat:value:#{key}")
      end

      def drop_by_resolution!(resolution)
        del_by_prefix("stat:value:#{key}:data:#{resolution}")
      end

      def storage_key(resolution, timestamp)
        formatted_time = Tabs::Resolution.serialize(resolution, timestamp)
        "stat:value:#{key}:data:#{resolution}:#{formatted_time}"
      end

      private

      def update_values(stat_key, value)
        hash = get_current_hash(stat_key)
        increment(hash, value)
        set(stat_key, JSON.generate(hash))
      end

      def delete_values(stat_key, value)
        hash = get_current_hash(stat_key)
        decrement(hash, value)
        set(stat_key, JSON.generate(hash))
      end

      def get_current_hash(stat_key)
        hash = get(stat_key)
        return JSON.parse(hash) if hash
        default_value
      end

      def increment(hash, value)
        hash["count"] += 1
        hash["sum"] += value.to_i
      end

      def decrement(hash, value)
        if  (hash["sum"] - value.to_i) >= 0 || Tabs::Config.negative_metric
         hash["count"] -= 1
         hash["sum"] -= value.to_i
      end
      end

      def default_value(nil_value=nil)
        { "count" => 0, "sum" => 0}
      end

    end
  end
end
