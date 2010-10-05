#!/usr/bin/env ruby

require 'socket'
require 'pp'

module Gearman

# = Server
#
# == Description
# A client for managing Gearman job servers.
  class Server
    ##
    # Create a new client.
    #
    # @param job_servers  "host:port"; either a single server or an array
    # @param prefix       function name prefix (namespace)
    def initialize(servers)

      @servers = Array(servers) # "host:port"
    end

    attr_reader :servers

    ##
    # Get a socket for a job server.
    #
    # @param hostport  job server "host:port"
    # @return          a Socket

    def socket(hostport, num_retries=3)

      num_retries.times do |i|
        begin
          sock = TCPSocket.new(*hostport.split(':'))
        rescue Exception
        else
          return sock
        end
      end
      #signal_bad_server(hostport)
      nil
      #raise RuntimeError, "Unable to connect to job server #{hostport}"
    end

    ##
    # Sends a command to the server.
    #
    # @return a response string
    def send_command(socket, msg)
      response = ''
      socket.puts(msg)
      while true do
        if (buf = socket.recv_nonblock(65536) rescue nil)
          response << buf
          return response if response =~ /\n.\n$/
        end
      end
    end

    ##
    # Returns results of a 'status' command.
    #
    # @return a hash of abilities with queued, active and workers keys.
    def status
      result = {}
      @servers.each do |server|
        sock=socket server
        if sock.nil?
          result[server] = "unable to connect to server" if sock.nil?
        else
          result[server] = {}
          if response = send_command(sock, 'status')
            response.split("\n").each do |line|
              unless line == '.'
                func, queue, running, workers = line.split /\s+/
                result[server][func]={:queue => queue, :running => running, :workers => workers}
              end
            end
          else
            result[server][func] = 'No response from server when sent the status command'
          end #if
        end #if


      end #servers

      result
    end

    ##
    # parses a worker line from the 'workers' command
    #
    # @return a hash containing the worker information
    def parse_worker_line(line)
      puts line

      return {} if line == '.'

      parts =  line.split ' '
      fd = parts.shift
      host = parts.shift
      name = parts.shift
      if name == '-'
        #this is a client
        type = 'client'
        name = nil
        functions = Array.new()

      elsif name == ':'
        #this is a worker with no name
        type = 'worker'
        name = nil
        functions = parts
      else
        #worker  with name
        #get rid of the colon
        parts.shift
        type = 'worker'
        functions = parts
      end

      {:type => type, :host => host, :name => name, :functions => functions}

    end

    ##
    # Returns results of a 'workers' command.
    #
    # @return an array of worker hashes, containing host, status and functions keys.
    def workers
      result = {}

      @servers.each do |server|


        sock=socket server
        if sock.nil?
          result[server] = "unable to connect to server" if sock.nil?

        else
          result[server] = {}
          result[server][:workers] = []
          if response = send_command(sock, 'workers')
            response.split("\n").each do |line|
              workers =   parse_worker_line line unless line == '.'
              result[server][:workers] << workers
            end
          else
            result[server][:workers] = "No response from server"

          end #response
        end #if
      end #servers
      result
    end

  end #server
end #module


if $0 == __FILE__
  gearman_servers =  ["localhost:4730"]

  pp Gearman::Server.new(gearman_servers).status

end
