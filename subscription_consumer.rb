require 'poseidon'
require 'json'
require 'cassandra'
require 'pry'
require 'bigdecimal'
require 'waterdrop'

$cluster = ::Cassandra.cluster
$session = $cluster.connect('rating')

WaterDrop.setup do |config|
  config.send_messages = true
  config.connection_pool_size = 20
  config.connection_pool_timeout = 1
  config.kafka_hosts = ['localhost:9092']
  config.raise_on_failure = true
end

Struct.new("RatedUse", :telemetry_device_id, :telemetry_channel_id, :ended_at, :started_at, :read, :charge, :plan_symbol, :charge_type, :day, :op, :op_value, :period_check)

Dir["#{Dir.pwd}/lib/*.rb"].each {|file| require file }


StreamConsumer.new.fetch
