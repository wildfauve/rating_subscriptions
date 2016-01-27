class StreamConsumer

  def initialize
    @stream = Poseidon::PartitionConsumer.new("SubscriptionRater", "localhost", 9092, "rated_use", 0, :latest_offset) #:earliest_offset :latest_offset
    @handler = StreamHandler.new
  end

  def fetch
    puts "===> Starting Fetch Wait Loop"
    loop do
      messages = @stream.fetch
      messages.each do |m|
        @handler.process m
      end
    end
  end

end
