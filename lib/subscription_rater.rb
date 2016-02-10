class SubscriptionRater
=begin
MODEL
{"telemetry_device_id":"5c74fbe4-2084-4339-a3ef-23f8d4d7de55",
"telemetry_channel_id":"f7ded8d1-32fd-46a4-bfb4-e5539b0448e5",
"ended_at":"2015-09-11 13:59:59 UTC",
"started_at":"2015-09-11 13:30:00 UTC",
"read":"0.1E0",
"charge":"0.40123E0",
"charge_type":"network_extorsion",
"plan_symbol":"plan_9",
"kind":"rated_use_charge",
"plan":"plan_9"}
=end
#  telemetry_id, channel_op_code, supply_node_id, reading_time, read, state
  def initialize

  end

  def process(plan, message)
    rated_use = to_use(message)
    model = find_sub_period(rated_use)
    if model.new?
      create_subscription_period(rated_use, model)
    else
      update_subscription_period(rated_use, model)
    end
  end

  def find_sub_period(rated_use)
    SubscriptionModel.find(rated_use)
  end

# #<struct Struct::RatedUse
# Message from RatedUse
# telemetry_device_id="5c74fbe4-2084-4339-a3ef-23f8d4d7de55",
# telemetry_channel_id="f7ded8d1-32fd-46a4-bfb4-e5539b0448e5",
# ended_at=2015-09-11 12:59:59 UTC,
# started_at=2015-09-11 12:30:00 UTC,
# read=#<BigDecimal:7fae79057fb0,'0.0',9(18)>,
# charge=#<BigDecimal:7fae79057f60,'0.0',9(18)>,
# plan_symbol="plan_9",
# charge_type="network_extorsion",
# day=2015-09-11 00:00:00 +0000,
# op="inc",
# op_value=#<BigDecimal:7fae79057f10,'0.0',9(18)>>
# read_op: "dec",
# read_op_value: #<BigDecimal:7fae79057f10,'0.0',9(18)>>

  def create_subscription_period(rated_use, model)
    #model = SubscriptionModel.new
    init_params(model, rated_use)
    model.insert
  end


  #<SubscriptionModel:0x007fae7903f0c8
  # @charge_type="network_extorsion",
  # @day=2015-09-11 00:00:00 +0000,
  # @telemetry_channel_id="f7ded8d1-32fd-46a4-bfb4-e5539b0448e5",
  # @telemetry_device_id="5c74fbe4-2084-4339-a3ef-23f8d4d7de55",
  # @total_charge=#<BigDecimal:7fae7903df48,'0.40123E0',9(18)>,
  # @total_usage=#<BigDecimal:7fae7903e448,'0.1E0',9(18)>>
  def update_subscription_period(rated_use, model)
    # need a way of checking that the inc/dec message hasn't been received (a set of start times for the day perhaps)
    if rate_change(rated_use)
      # TODO: strange behaviour of cassandra driver leaves UTC times as NOT UTC
      if model.period_check.to_a.map {|t| t.gmtime}.include? rated_use.started_at  # so if we already have the period_check
        # TODO: READ needs ro have the same semantics
        puts "Applied #{rated_use.op} as #{rated_use.op_value}"
        rated_use.charge = determine_charge_change(rated_use) # the inc/dec can be obeyed
        rated_use.read = determine_read_change(rated_use)
      end
    end
    update_params(model, rated_use)
    model.update
  end

  def rate_change(rated_use)
    case rated_use.op
    when "inc", "dec"
      true
    when nil
      false
    else
      binding.pry
    end
  end

  def determine_charge_change(rated_use)
    rated_use.op == "inc" ? rated_use.op_value : rated_use.op_value * -1
  end

  def determine_read_change(rated_use)
    rated_use.read_op == "inc" ? rated_use.read_op_value : rated_use.read_op_value * -1
  end


  def init_params(m, rated_use)
    set_common(m, rated_use)
    m.period_check = Set.new([rated_use.started_at])
    m.total_charge = rated_use.charge
    m.total_usage = rated_use.read
  end

  def update_params(m, rated_use)
    set_common(m, rated_use)
    m.period_check.add rated_use.started_at
    m.total_charge += rated_use.charge
    m.total_usage += rated_use.read
  end

  def set_common(m, rated_use)
    m.telemetry_device_id = rated_use.telemetry_device_id
    m.telemetry_channel_id = rated_use.telemetry_channel_id
    m.day = rated_use.day
    m.charge_type = rated_use.charge_type
  end


  def build_charge_event(model, plan)
    model.new? ? model.to_hash.merge(headers(plan)) : model.to_hash.merge(headers(plan)).merge(determine_op(model))
  end

  def headers(plan)
    {
      kind: "rated_use_charge",
      plan: plan.symbol,
      charge_type: plan.charge_type
    }

  end

  def to_use(message)
    r = Struct::RatedUse.new
    r.telemetry_device_id = message["telemetry_device_id"]
    r.telemetry_channel_id = message["telemetry_channel_id"]
    r.ended_at = to_time(message["ended_at"])
    r.started_at = to_time(message["started_at"])
    r.read = to_dec(message["read"])
    r.charge = to_dec(message["charge"])
    r.plan_symbol = message["plan_symbol"]
    r.charge_type = message["charge_type"]
    r.op = message["op"] if message["op"]
    r.op_value = to_dec(message["op_value"]) if message["op_value"]
    r.day = day_from_time(r.started_at)
    r
  end

  def to_time(t)
    Time.parse(t)
  end

  def to_dec(n)
    BigDecimal.new(n,4)
  end

  def day_from_time(time)
    Time.new(time.year, time.month, time.day, 0,0,0,"+00:00")
  end


end
