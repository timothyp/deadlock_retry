require 'active_support/core_ext/module/attribute_accessors'

module DeadlockRetry
  DEFAULT_MAXIMUM_RETRIES_ON_DEADLOCK = 3

  mattr_accessor :maximum_retries_on_deadlock

  DEADLOCK_ERROR_MESSAGES = [
    "Deadlock found when trying to get lock",
    "Lock wait timeout exceeded",
    "deadlock detected",
    "detected deadlock"
  ]

  DeadlockRetry.maximum_retries_on_deadlock ||= DeadlockRetry::DEFAULT_MAXIMUM_RETRIES_ON_DEADLOCK

  def transaction(*objects, &block)
    retry_count = 0

    begin
      super
    rescue ActiveRecord::StatementInvalid, ActiveRecord::Deadlocked => error
      raise if in_nested_transaction?
      if DEADLOCK_ERROR_MESSAGES.any? { |msg| error.message =~ /#{Regexp.escape(msg)}/ }
        retries_exhausted = retry_count >= DeadlockRetry.maximum_retries_on_deadlock
        logger.info "Deadlock detected on attempt #{retry_count + 1}. Max retries: #{DeadlockRetry.maximum_retries_on_deadlock}, so #{'not ' if retries_exhausted}restarting transaction. Exception: #{error.to_s}"
        raise if retries_exhausted
        retry_count += 1
        exponential_pause(retry_count)
        retry
      else
        raise
      end
    end
  end

  private

  WAIT_TIMES = [0, 0.1, 0.2, 0.4, 0.8, 1.6, 3.2]
  MAX_WAIT_TIME = 5

  def exponential_pause(count)
    sec = WAIT_TIMES[count - 1] || MAX_WAIT_TIME
    # Sleep for a longer time each attempt.
    # Cap the pause time at MAX_WAIT_TIME seconds.
    sleep(sec) if sec != 0
  end

  def in_nested_transaction?
    # open_transactions was added in 2.2's connection pooling changes.
    connection.open_transactions != 0
  end
end

ActiveRecord::Base.singleton_class.prepend(DeadlockRetry) if defined?(ActiveRecord)
