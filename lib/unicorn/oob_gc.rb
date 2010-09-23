# -*- encoding: binary -*-
module Unicorn

  # Run GC after every request, after closing the client socket and
  # before attempting to accept more connections.
  #
  # This shouldn't hurt overall performance as long as the server cluster
  # is at <50% CPU capacity, and improves the performance of most memory
  # intensive requests.  This serves to improve _client-visible_
  # performance (possibly at the cost of overall performance).
  #
  # We'll call GC after each request is been written out to the socket, so
  # the client never sees the extra GC hit it.
  #
  # This middleware is _only_ effective for applications that use a lot
  # of memory, and will hurt simpler apps/endpoints that can process
  # multiple requests before incurring GC.
  #
  # This middleware is only designed to work with Unicorn, as it harms
  # keepalive performance.
  #
  # Example (in config.ru):
  #
  #     require 'unicorn/oob_gc'
  #
  #     # GC ever two requests that hit /expensive/foo or /more_expensive/foo
  #     # in your app.  By default, this will GC once every 5 requests
  #     # for all endpoints in your app
  #     use Unicorn::OobGC, 2, %r{\A/(?:expensive/foo|more_expensive/foo)}
  #     use Unicorn::OobGC, [OobGCCondition::ForPath(%r{\A/(?:expensive/foo|more_expensive/foo)}), OobGCCondition::OnInterval(2)]
  #
  # Generally speaking, you want to apply your Conditions in order from most restrictive to least restrictive.
  class OobGC
    attr_accessor :app, :env, :body

    def initialize(app)
      self.app = app
    end

    def call(env)
      self.env = env

      before_request
      status, headers, self.body = app.call(env)
      after_request
      
      [ status, headers, self ]
    end

    def each(&block)
      body.each(&block)
    end

    # in Unicorn, this is closed _after_ the client socket
    def close
      body.close if body.respond_to?(:close)
      gc if should_gc?
    end

    def before_request
      GC.disable
    end

    def should_gc?
      true
    end

    def gc
      self.body = nil
      env.clear
      GC.enable
      GC.start
      GC.disable
    end

    def after_request
    end

  end


  class OobGCForInterval < OobGC
    attr_accessor :requests_since_gc, :interval

    def initialize(app, interval)
      super(app)
      self.interval = self.requests_since_gc = interval
    end

    def should_gc?
      return false if ((self.requests_since_gc -= 1) > 0)

      self.requests_since_gc = self.interval

      return true
    end

  end

  class OobGCForPath < OobGC
    attr_accessor :path
    
    def initialize(app, path)
      super(app)
      self.path = path
    end

    def should_gc?
      path =~ self.env['PATH_INFO']
    end
  end

  class OobMemoryLeakFinder < OobGC
    attr_accessor :profiler_before_request, :profiler_after_gc, :objects_before_request, :objects_after_request, :objects_after_gc, :request_path, :path_parameters, :history, :logger, :profile_worker
    require 'logger'
    
    def initialize(app, profile_worker=1)
      self.history = Hash.new
      self.profile_worker = profile_worker.to_s
      super(app)
    end

    def should_gc?
      profile_worker == worker_number #worker_number == "1" || worker_number == "2"
    end
    
    def worker_number
      # this will do something bad if env isn't set yet
      self.env[Const::UNICORN_WORKER].to_s
    end

    def controller_action
      "#{self.path_parameters["controller"]} #{self.path_parameters["action"]}"
    end

    def before_request
      return unless should_gc?
      GC::Profiler.enable
      
      if self.logger.nil?
        self.logger = Logger.new("/var/log/unicorn/memory_leak.#{worker_number}.log")
        self.logger.formatter = Logger::Formatter.new
        self.logger.info ", request_path, controller, action, delta_objects, objects_before_request, objects_after_request}, objects_after_gc, invokes, before_use_bytes, after_use_bytes, before_total_bytes, after_total_bytes, before_total_objects, after_total_objects, after_gc_ms"
      end

      self.request_path = self.env['PATH_INFO']
      self.objects_before_request =  ObjectSpace.count_objects[:TOTAL]
      self.profiler_before_request = most_recent_gc_profiler_results
    end

    def after_request
      return unless should_gc?
      self.objects_after_request =  ObjectSpace.count_objects[:TOTAL]
      self.path_parameters = self.env["action_controller.request.path_parameters"]
    end

    def gc
      super 
      objects_after_gc =  ObjectSpace.count_objects[:TOTAL]
      self.profiler_after_gc = most_recent_gc_profiler_results

      self.history["#{controller_action}"] ||= Array.new
      self.history["#{controller_action}"] << objects_after_gc - objects_before_request

      self.logger.info ", #{self.request_path}, #{self.path_parameters["controller"]}, #{self.path_parameters["action"]}, #{objects_after_gc - objects_before_request}, #{objects_before_request}, #{objects_after_request}, #{objects_after_gc}, #{profiler_after_gc["Invokes"]}, #{profiler_before_request["Use Size(byte)"]}, #{profiler_after_gc["Use Size(byte)"]}, #{profiler_before_request["Total Size(byte)"]}, #{profiler_after_gc["Total Size(byte)"]}, #{profiler_before_request["Total Object"]}, #{profiler_after_gc["Total Object"]}, #{profiler_after_gc["GC Time(ms)"].to_i}"
      
    end

    def most_recent_gc_profiler_results
      # ew, fugly
      results = Hash.new
      s = GC::Profiler.result.split("\n")
      
      return results if s.empty?
      
      results['Invokes'] = s[0][/\d+/]
      headers = s[1].split(/ \s+/)
      data = s[s.length-1].split(" ")
      headers.each_with_index {|header, index| results[header] = data[index] }
      results
    end
  end

  class OobGCDisabled < OobGC
    def gc
      # do nothing
    end
  end

  # superclasses OobGCForInterval as a backup strategy if the max begins to fail
  class OobGCOnMaxMemory < OobGCForInterval
    attr_accessor :max_memory, :successful

    def initialize(app, max_memory, fallback_interval=5)
      super(app, fallback_interval)
      self.max_memory = max_memory.to_i
      self.successful = true
    end

    def should_gc?
      return true if (successful and (get_memory_usage > max_memory)) 

      # fallback to interval if this max memory strategy isn't working anymore
      super
    end

    def after_gc
      # check to see if we are running a successful stragegy still
      # if we are over the max after GC, then we can't use this strategy anymore
      # we still would need to call GC from time to time
      self.successful = get_memory_usage < max_memory
    end

    #copied from oink: http://github.com/marshally/oink/blob/master/lib/oink/rails/memory_usage_logger.rb
    def get_memory_usage
      if defined? WIN32OLE
        wmi = WIN32OLE.connect("winmgmts:root/cimv2")
        mem = 0
        query = "select * from Win32_Process where ProcessID = #{$$}"
        wmi.ExecQuery(query).each do |wproc|
          mem = wproc.WorkingSetSize
        end
        mem.to_i / 1000
      elsif proc_file = File.new("/proc/#{$$}/smaps") rescue nil
        proc_file.map do |line|
          size = line[/Size: *(\d+)/, 1] and size.to_i
        end.compact.sum
      elsif proc_file = File.new("/proc/#{$$}/status") rescue nil
        proc_file.map do |line|
          size = line[/VmSize:\s*(\d+)/, 1] and size.to_i
        end.compact.sum
      else
        `ps -o vsz= -p #{$$}`.to_i
      end
    end

  end

end
