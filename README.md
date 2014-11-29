# gearman-ruby

## What is this?

This is a pure-Ruby library for the [gearman][Gearman] distributed job system.

## What needs to be done?

More testing, some code cleanup.

## What's in this?

Right now, this library has both client and worker support for Ruby apps.

## Getting Started

### Client

A very simple client that submits a "sleep" job and waits for 100 seconds for results:

``` ruby
require 'rubygems'
require 'gearman'

servers = ['localhost:4730', 'localhost:4731']

client = Gearman::Client.new(servers)
taskset = Gearman::TaskSet.new(client)

task = Gearman::Task.new('sleep', 20)
task.on_complete {|d| puts d }

taskset.add_task(task)
taskset.wait(100)
```

### Worker

A worker that will process jobs in the 'sleep' queue:

``` ruby
require 'rubygems'
require 'logger'
require 'gearman'

servers = ['localhost:4730']

w = Gearman::Worker.new(servers)
logger = Logger.new(STDOUT)

# Add a handler for a "sleep" function that takes a single argument, the
# number of seconds to sleep before reporting success.
w.add_ability("sleep") do |data,job|
 seconds = 10
 logger.info "Sleeping for #{seconds} seconds"
 (1..seconds.to_i).each do |i|
   sleep 1
   # Report our progress to the job server every second.
   job.report_status(i, seconds)
 end
 # Report success.
 true
end

loop { w.work }
```

[gearman]: http://gearman.org

## Authors

* John Ewart <john@johnewart.net> (current maintainer, author of re-write)

<<<<<<< HEAD
## Contributors (past and present)

* Kim Altintop
=======
## Contributors

>>>>>>> New version (4.0) -- substantial rewrite
* Josh Black (raskchanky)
* Colin Curtin (perplexes)
* Brian Cobb (bcobb)
* Pablo A. Delgado (pablete)
<<<<<<< HEAD
* Daniel Erat
* Antonio Garrote 
* Stefan Kaes (skaes)
* Ladislav Martincik
=======
* Stefan Kaes (skaes)
>>>>>>> New version (4.0) -- substantial rewrite
* Mauro Pompilio (malditogeek)
* Lee Reilly (leereilly)
* Clint Shryock (catsby)
* Andy Triggs (andyt)


<<<<<<< HEAD

## License

Released under the MIT license, originally developed by XING AG. See the LICENSE file for further details.
=======
## License

Released under the MIT license. See the file LICENSE for further details.
>>>>>>> New version (4.0) -- substantial rewrite
