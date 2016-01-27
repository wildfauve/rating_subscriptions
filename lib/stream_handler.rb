class StreamHandler

  def initialize
    @parser = :json
    @sink = SubscriptionRater.new
    @plan = Plan.new
  end

  def process(raw_msg)
    @sink.process(@plan, self.send(@parser, raw_msg.value))
  end

  def json(raw)
    JSON.parse(raw)
  end

end
