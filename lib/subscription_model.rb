class SubscriptionModel
=begin

CREATE TABLE subscription_charges (
  telemetry_device_id text,
  telemetry_channel_id text,
  charge_type text,
  day timestamp,
  total_usage decimal,
  total_charge decimal,
  period_check set<timestamp>,
  PRIMARY KEY (telemetry_device_id, telemetry_channel_id, charge_type, day)
);

=end

  class << self

    def find(usage)
      f = $session.execute(self.statement(:find), arguments: [
          usage.telemetry_device_id,
          usage.telemetry_channel_id,
          usage.charge_type,
          usage.day
        ])
      raise if f.rows.size > 1
      f.rows.size == 0 ? self.new : self.new.init(f.rows.first)
    end

    def create
      self.new
    end


  end

  include Kaftan

  table :subscription_charges

  prepare :find, "SELECT * from subscription_charges WHERE telemetry_device_id = ? AND telemetry_channel_id = ? AND charge_type = ? AND day = ?"

  field :telemetry_device_id, type: :text, key: true
  field :telemetry_channel_id, type: :text, key: true
  field :charge_type, type: :text, key: true
  field :day, type: :time, with: :day_from_time, key: true
  field :total_usage, type: :decimal
  field :total_charge, type: :decimal
  field :period_check, type: :set, member_type: :time

  build_default_prepares

  def initialize
    self
  end


end
