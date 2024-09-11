require 'rubygems'

# Change the version if you want to test a different version of ActiveRecord
gem 'activerecord', ENV['ACTIVERECORD_VERSION'] || ' ~>6.1.0'
require 'active_record'
require 'active_record/version'
puts "Testing ActiveRecord #{ActiveRecord::VERSION::STRING}"

require 'minitest/autorun'
require 'mocha/minitest'
require 'logger'
require "deadlock_retry"

class MockModel
  @@open_transactions = 0

  def self.transaction(**options, &block)
    @@open_transactions += 1
    yield
  ensure
    @@open_transactions -= 1
  end

  def self.open_transactions
    @@open_transactions
  end

  def self.connection
    self
  end

  def self.logger
    @logger ||= Logger.new(nil)
  end

  def self.select_one(sql)
    {'Type' => '', 'Name' => '', 'Status' => 'INNODB STATUS INFO'}
  end

  def self.select_rows(sql)
    [['version', '5.5']]
  end

  def self.select_value(sql)
    true
  end

  def self.adapter_name
    "Mysql2"
  end

  include DeadlockRetry
end

class MockModelOldMySQL < MockModel
  def self.select_one(sql)
    {'Status' => 'OLD INNODB STATUS INFO'}
  end

  def self.select_rows(sql)
    [['version', '5.1.45']]
  end

  def self.adapter_name
    "MySQL"
  end
end

class DeadlockRetryTest < Minitest::Test
  DEADLOCK_ERROR = "MySQL::Error: Deadlock found when trying to get lock"
  TIMEOUT_ERROR = "MySQL::Error: Lock wait timeout exceeded"

  def setup
    MockModel.stubs(:exponential_pause)
    @events = []
    ActiveSupport::Notifications.subscribe('deadlock_retry') { |*args| @events << ActiveSupport::Notifications::Event.new(*args) }
  end

  def test_no_errors
    assert_equal :success, MockModel.transaction { :success }
    assert_equal 0, @events.size
  end

  def test_no_errors_with_deadlock
    errors = [ DEADLOCK_ERROR ] * 3
    assert_equal :success, MockModel.transaction { raise ActiveRecord::StatementInvalid, errors.shift unless errors.empty?; :success }
    assert errors.empty?
    assert_equal 3, @events.size
    assert_equal({attempt: 1, type: :deadlock, retries_exhausted: false}, @events[0].payload)
    assert_equal({attempt: 2, type: :deadlock, retries_exhausted: false}, @events[1].payload)
    assert_equal({attempt: 3, type: :deadlock, retries_exhausted: false}, @events[2].payload)
  end

  def test_no_errors_with_lock_timeout
    errors = [ TIMEOUT_ERROR ] * 3
    assert_equal :success, MockModel.transaction { raise ActiveRecord::StatementInvalid, errors.shift unless errors.empty?; :success }
    assert errors.empty?
    assert_equal 3, @events.size
    assert_equal({attempt: 1, type: :lock_wait_timeout, retries_exhausted: false}, @events[0].payload)
    assert_equal({attempt: 2, type: :lock_wait_timeout, retries_exhausted: false}, @events[1].payload)
    assert_equal({attempt: 3, type: :lock_wait_timeout, retries_exhausted: false}, @events[2].payload)
  end

  def test_error_if_limit_exceeded
    MockModel.expects(:log_innodb_status).times(4)

    attempts = 0
    assert_raises(ActiveRecord::StatementInvalid) do
      MockModel.transaction { attempts += 1; raise ActiveRecord::StatementInvalid, DEADLOCK_ERROR }
    end

    assert_equal 4, attempts
    assert_equal 4, @events.size
    assert_equal({attempt: 1, type: :deadlock, retries_exhausted: false}, @events[0].payload)
    assert_equal({attempt: 2, type: :deadlock, retries_exhausted: false}, @events[1].payload)
    assert_equal({attempt: 3, type: :deadlock, retries_exhausted: false}, @events[2].payload)
    assert_equal({attempt: 4, type: :deadlock, retries_exhausted: true}, @events[3].payload)
  end

  def test_adjusted_maximum_retries
    DeadlockRetry.maximum_retries_on_deadlock = 5

    MockModel.expects(:log_innodb_status).times(6)

    attempts = 0
    assert_raises(ActiveRecord::StatementInvalid) do
      MockModel.transaction { attempts += 1; raise ActiveRecord::StatementInvalid, DEADLOCK_ERROR }
    end

    assert_equal 6, attempts
    assert_equal 6, @events.size
  ensure
    DeadlockRetry.maximum_retries_on_deadlock = DeadlockRetry::DEFAULT_MAXIMUM_RETRIES_ON_DEADLOCK
  end

  def test_no_retries
    DeadlockRetry.maximum_retries_on_deadlock = 0

    MockModel.expects(:log_innodb_status).once

    attempts = 0
    assert_raises(ActiveRecord::StatementInvalid) do
      MockModel.transaction { attempts += 1; raise ActiveRecord::StatementInvalid, DEADLOCK_ERROR }
    end

    assert_equal 1, attempts
    assert_equal 1, @events.size
    assert_equal({attempt: 1, type: :deadlock, retries_exhausted: true}, @events[0].payload)
  ensure
    DeadlockRetry.maximum_retries_on_deadlock = DeadlockRetry::DEFAULT_MAXIMUM_RETRIES_ON_DEADLOCK
  end

  def test_error_if_unrecognized_error
    assert_raises(ActiveRecord::StatementInvalid) do
      MockModel.transaction { raise ActiveRecord::StatementInvalid, "Something else" }
    end
    assert_equal 0, @events.size
  end

  def test_included_by_default
    assert ActiveRecord::Base.respond_to?(:transaction_with_deadlock_handling)
  end

  def test_innodb_status_availability
    DeadlockRetry.innodb_status_cmd = nil
    MockModel.transaction {}
    assert_equal "show engine innodb status", DeadlockRetry.innodb_status_cmd
  end

  def test_innodb_status_availability_for_old_mysql
    DeadlockRetry.innodb_status_cmd = nil
    MockModelOldMySQL.transaction {}
    assert_equal "show innodb status", DeadlockRetry.innodb_status_cmd
  end

  def test_show_innodb_status
    seq = sequence('logging')
    deadlock_id = "1234abcd"
    MockModel.expects(:random_deadlock_id).returns(deadlock_id)
    MockModel.logger.expects(:info).in_sequence(seq).with(initial_log_message(DEADLOCK_ERROR))
    MockModel.logger.expects(:info).in_sequence(seq).with("(INNODB #{deadlock_id}) Status follows:")
    MockModel.logger.expects(:info).in_sequence(seq).with("(INNODB #{deadlock_id}) INNODB STATUS INFO")

    errors = [DEADLOCK_ERROR]
    MockModel.transaction { raise ActiveRecord::StatementInvalid, errors.shift unless errors.empty?; :success }
    assert_equal 1, @events.size
  end

  def test_show_innodb_status_for_old_mysql
    seq = sequence('logging')
    deadlock_id = "7890cdef"
    MockModelOldMySQL.expects(:random_deadlock_id).returns(deadlock_id)
    MockModelOldMySQL.logger.expects(:info).in_sequence(seq).with(initial_log_message(DEADLOCK_ERROR))
    MockModelOldMySQL.logger.expects(:info).in_sequence(seq).with("(INNODB #{deadlock_id}) Status follows:")
    MockModelOldMySQL.logger.expects(:info).in_sequence(seq).with("(INNODB #{deadlock_id}) OLD INNODB STATUS INFO")

    errors = [DEADLOCK_ERROR]
    MockModelOldMySQL.transaction { raise ActiveRecord::StatementInvalid, errors.shift unless errors.empty?; :success }
    assert_equal 1, @events.size
  end

  def test_error_in_nested_transaction_should_retry_outermost_transaction
    tries = 0
    errors = 0

    MockModel.transaction do
      tries += 1
      MockModel.transaction do
        MockModel.transaction do
          errors += 1
          raise ActiveRecord::StatementInvalid, "MySQL::Error: Lock wait timeout exceeded" unless errors > 3
        end
      end
    end

    assert_equal 4, tries
  end

  private

  def initial_log_message(error)
    "Deadlock detected on attempt 1. Max retries: 3, so restarting transaction. Exception: #{error.to_s}"
  end

end
