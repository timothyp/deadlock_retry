require 'active_support/core_ext/module/attribute_accessors'

module DeadlockRetry
  def self.included(base)
    base.extend(ClassMethods)
    base.class_eval do
      class << self
        alias_method_chain :transaction, :deadlock_handling
      end
    end
  end

  DEFAULT_MAXIMUM_RETRIES_ON_DEADLOCK = 3

  mattr_accessor :maximum_retries_on_deadlock
  mattr_accessor :innodb_status_cmd

  module ClassMethods
    DEADLOCK_ERROR_MESSAGES = [
      "Deadlock found when trying to get lock",
      "Lock wait timeout exceeded",
      "deadlock detected",
      "detected deadlock"
    ]

    DeadlockRetry.maximum_retries_on_deadlock ||= DeadlockRetry::DEFAULT_MAXIMUM_RETRIES_ON_DEADLOCK

    def transaction_with_deadlock_handling(*objects, &block)
      retry_count = 0

      check_innodb_status_available

      begin
        transaction_without_deadlock_handling(*objects, &block)
      rescue ActiveRecord::StatementInvalid, ActiveRecord::Deadlocked => error
        raise if in_nested_transaction?
        if DEADLOCK_ERROR_MESSAGES.any? { |msg| error.message =~ /#{Regexp.escape(msg)}/ }
          retries_exhausted = retry_count >= DeadlockRetry.maximum_retries_on_deadlock
          logger.info "Deadlock detected on attempt #{retry_count + 1}. Max retries: #{DeadlockRetry.maximum_retries_on_deadlock}, so #{'not ' if retries_exhausted}restarting transaction. Exception: #{error.to_s}"
          log_innodb_status if DeadlockRetry.innodb_status_cmd
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

    def show_innodb_status
      self.connection.select_one(DeadlockRetry.innodb_status_cmd)['Status']
    end

    # Should we try to log innodb status -- if we don't have permission to,
    # we actually break in-flight transactions, silently (!)
    def check_innodb_status_available
      return unless DeadlockRetry.innodb_status_cmd == nil

      if self.connection.adapter_name.downcase.include?('mysql')
        begin
          mysql_version = self.connection.select_rows('show variables like \'version\'')[0][1]
          cmd = if mysql_version < '5.5'
            'show innodb status'
          else
            'show engine innodb status'
          end
          self.connection.select_value(cmd)
          DeadlockRetry.innodb_status_cmd = cmd
        rescue
          logger.info "Cannot log innodb status: #{$!.message}"
          DeadlockRetry.innodb_status_cmd = false
        end
      else
        DeadlockRetry.innodb_status_cmd = false
      end
    end

    def log_innodb_status
      # Showing the innodb status is the only way to get visiblity into why the
      # transaction deadlocked.  Log it, along with a prefix including an id to
      # enable easy extraction of the resulting status dump from the log.
      lines = show_innodb_status
      deadlock_id = SecureRandom.hex(4)
      logger.info "(INNODB #{deadlock_id}) Status follows:"
      lines.each_line do |line|
        logger.info "(INNODB #{deadlock_id}) " + line
      end
    rescue => e
      # Access denied, ignore
      logger.info "Cannot log innodb status: #{e.message}"
    end

  end
end

ActiveRecord::Base.send(:include, DeadlockRetry) if defined?(ActiveRecord)
