require 'socket'

server = TCPServer.open 10034
loop {
  client = server.accept
  client.puts "1,mark"
  client.puts "2,jim"
  client.close
}
