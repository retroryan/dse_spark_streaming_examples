require 'socket'

server = TCPServer.open 10034
loop{
    client = server.accept
    client.puts "999-22-1234,John Smith,BigLots,12.34,2014-01-05"
    client.puts "999-22-1234,John Smith,BigLots,9999.34,2014-01-06"
    client.puts "999-22-1234,John Smith,TheVerge,100,2014-01-07"
    client.puts "999-22-1235,Mark Smith,BigLots,9999.34,2014-01-06"
    client.puts "999-22-1235,Mark Smith,TheVerge,100,2014-01-07"
    client.close
  }
