class Plan

  attr_accessor :charge

  def initialize
  end

  def rate(reading)
    reading["value"].class == BigDecimal ? @use = reading["value"] : @use = BigDecimal.new(reading["value"], 6)
    @charge = @use * charge_rate()
    self
  end

  def value
    @charge
  end

  def symbol
    :plan_9
  end

  def charge_type
    :network_extorsion
  end

  def charge_rate
    BigDecimal(4.0123, 6)
  end

end
