require 'socket'

server = TCPServer.open 10040
loop {
  client = server.accept
  client.puts "1,this is marks blog"
  client.puts "2,this is jims blog"
  client.close
}
