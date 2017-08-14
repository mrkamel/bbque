# BBQue

[![Build Status](https://secure.travis-ci.org/mrkamel/bbque.png?branch=master)](http://travis-ci.org/mrkamel/bbque)
[![Gem Version](https://badge.fury.io/rb/bbque.svg)](http://badge.fury.io/rb/bbque)

BBQue is an opinionated ruby gem to queue and process background jobs. Other
gems for this purpose usually don't work with ruby objects and serialize method
arguments only. Instead, BBQue jobs are simple ruby objects:

```ruby
MyQueue.enqueue MyJob.new
```

BBQue jobs need to fulfill the following interface:

1. The object contains an instance method `#work` without any arguments
2. The object (instance) must be serializable via `Marshal.dump` and `Marshal.load`

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'bbque'
```

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install bbque

## Usage

BBQue is using Redis. Therefore, you have to install Redis first.

### Producer

To enqueue a job, you need a Producer instance:

```ruby
SomeQueue = BBQue::Producer.new("default")
```

where `default` is the queue name. You can additionally pass a logger and/or the Redis instance:

```ruby
SomeQueue = BBQue::Producer.new("default", redis: Redis.new, logger: Logger.new(...))
```

Then enqueue a job:

```ruby
SomeQueue.enqueue MyJob.new("Some argument")
```

where `MyJob` looks like:

```ruby
class MyJob
  attr_accessor :argument

  def initialize(argument)
    self.argument = argument
  end

  def work
    # Do some work
  end
end
```

### Consumer

To process the enqueued jobs, create a file, e.g. jobs.rb:

```ruby
BBQue::Consumer.new("default").run
```

and run it via:

    $ bbque jobs.rb

BBQue will loop through all the jobs, run them, and will then wait for new
ones (no polling). You can pass the queue name, the Redis instance and a logger.

```ruby
BBQue::Consumer.new(["default", "important"], logger: ..., redis: ...).run
```

### Forking Consumers

By default, BBQue does not fork a process for every job, but
it already ships with the ability to do so.

To make BBQue fork, create a custom consumer:

```ruby
class ForkingConsumer < BBQue::Consumer
  def fork?
    true
  end

  def before_fork
    # ...
  end

  def after_fork
    # ...
  end
end
```

Then, start your custom consumer in favor of BBQue's default one:

```ruby
ForkingConsumer.new(["queue1", "queue2", ...], ...).run
```

## Job Limit

TODO

## Delay

TODO

## Graceful Termination

You can stop a worker gracefully by sending a QUIT signal to it.
The worker will finish its current job and terminate afterwards.

## Contributing

1. Fork it ( https://github.com/mrkamel/bbque/fork )
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request
