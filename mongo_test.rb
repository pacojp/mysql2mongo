require 'fileutils'
require 'mongo'
require 'pp'

#
# thanks
# http://d.hatena.ne.jp/matsukaz/20110417/1303057728
#

SHARD_COUNT  = 3
CHUNK_SIZE   = 3 # mega-byte
MONGOS_COUNT = 3
TEST_DIR     = '/tmp/mongo_test'
IP_ADDRESS   = '192.168.100.145'

#
# the one's digit means active replica
# the ten's digit means replicaset id
# the handred's digit means hardwares(servers).
# the thoudans's digit means service type(config mongos mongod). (because web console uses service port + 1000)
#
# config
#   31001
# mongos
#   33101
#   33201
#   .
#
# replicaSets
#   35111,35210,35310
#   35120,35221,35320
#   35130,35230,35331
#   .
#

raise 'SHARD_COUNT must be (1..9)' unless (1..9).include?(SHARD_COUNT)
raise 'MONGOS_COUNT must be (1..9)' unless (1..9).include?(MONGOS_COUNT)
raise 'TEST_DIR error' if TEST_DIR == nil || TEST_DIR.size == 0 || TEST_DIR == '/'

@commands = []

def start_server(cmd,port)
  puts cmd
  @commands << cmd
  fork{`#{cmd}`}

  sleep 0.5

  puts "checking mongod(mongos) is active at #{port}"
  loop do
    begin
      mongo = Mongo::Connection.new(IP_ADDRESS,port)
      puts 'ok'
      break
    rescue
      print "."
      sleep 1
    end
  end
end

def bar
  puts '=' * 80
end

class Base
  attr_accessor :id,:name,:port,:hardware
  def initialize(hash)
    self.id = hash[:id]
    self.name = hash[:name]
    self.port = hash[:port]
    self.hardware = hash[:hardware]
  end
end

class ConfigServer < Base
  def initialize(hash)
    super(hash)
  end
end

class Mongos < Base
  def initialize(hash)
    super(hash)
  end
end

class Cluster
  attr_accessor :config,:mongos,:shards
  def initialize
    self.config = nil
    self.mongos = []
    self.shards = []
  end

  def all_mongod
    ret = [self.config]
    self.shards.each do |shard|
      shard.replicaset.each do |repl|
        ret << repl
      end
    end
    ret
  end
end

class Shard
  attr_accessor :replicaset,:name
  def initialize
    self.replicaset = Replicaset.new
  end

  def replica_config_hash
    ret = {'_id'=>self.name,'members'=>[]}
    replicaset.each do |repl|
      ret['members'] << {'_id'=>repl.id,'host'=>"#{IP_ADDRESS}:#{repl.port}"}
    end
    ret
  end

  def shard_config_string
    "#{self.name}/#{replicaset.map{|repl| "#{IP_ADDRESS}:#{repl.port}"}.join(',')}"
  end
end

class Replicaset < Array
  def primary
    self.select{|o|o.primary}.first
  end
end

class Replica < Base
  attr_accessor :primary
  def initialize(hash)
    super(hash)
    self.primary = hash[:primary]
  end
end

cluster = Cluster.new
cluster.config = ConfigServer.new(:id=>0,:port=>31001,:hardware=>0,:name=>'config1')

(1..MONGOS_COUNT).each do |i|
  port = 33001 + (i * 100)
  cluster.mongos << Mongos.new(:id=>i - 1 ,:port=> (port),:hardware=>i,:name=>"mongos#{i}")
end

(1..SHARD_COUNT).each do |i|
  shard = Shard.new
  shard.name = "s#{i}"
  (1..3).each do |j|
    port = 35000 + (j * 100) # it does not mean sharding mechanism uses port. see code below.
    port = port + (i * 10)
    primary = i == j
    port += 1 if primary
    shard.replicaset << Replica.new(:id => j - 1,:name=>"r#{j}",:primary=>primary,:port=>port,:hardware=>j)
  end
  cluster.shards << shard
end

bar
pp cluster
STDOUT.flush
bar

# create config
port = cluster.config.port
hardware = cluster.config.hardware
dir = "#{TEST_DIR}/dbs/config#{port}"
FileUtils.mkdir_p(dir)
puts "start config at hardware:#{hardware}"
cmd = "mongod --dbpath=#{dir} --configsvr --port=#{port} > #{TEST_DIR}/config_#{port}.log"
start_server(cmd,port)


# create mongod
cluster.shards.each do |shard|
  shard_name = shard.name
  shard.replicaset.each do |repl|
    port = repl.port
    dir = "#{TEST_DIR}/dbs/#{shard_name}_#{repl.name}"
    FileUtils.mkdir_p(dir)
    puts "start mongod at hardware:#{repl.hardware}(#{repl.primary ? 'primary':'secondary'})"
    #cmd = "mongod --dbpath=#{dir} --replSet #{shard_name} --port=#{port} --rest > #{TEST_DIR}/mongod_#{port}.log"
    cmd = "mongod --dbpath=#{dir} --replSet #{shard_name} --port=#{port} > #{TEST_DIR}/mongod_#{port}.log"
    start_server(cmd,port)
  end
end

bar

# setup replicasets
cluster.shards.each do |shard|
  primary = shard.replicaset.primary
  if primary
    puts "setup replicaset at #{shard.name}"
    mongo = Mongo::Connection.new(IP_ADDRESS,primary.port)
    mongo = mongo['admin']
    p shard.replica_config_hash
    mongo.command({'replSetInitiate'=>shard.replica_config_hash})
  else
    raise 'must not happen (there should be primary replica at least)'
  end
end

bar

# wait until replicasets created
cluster.shards.each do |shard|
  primary = shard.replicaset.primary
  if primary
    puts "check replicaset at #{shard.name}"
    loop do
      begin
        mongo = Mongo::Connection.new(IP_ADDRESS,primary.port)
        status = mongo['admin'].command({'replSetGetStatus'=>1})
        if status['ok'].to_i == 1
          puts "ok"
          break
        end
      rescue
      end
      sleep 1
      print '.'
    end
  else
    raise 'must not happen (there should be primary replica at least)'
  end
end

puts 'all replicaset is up'
bar

# create mongos (i got error when create mongos before creating replicaset)
cluster.mongos.each do |mongos|
  port = mongos.port
  hardware = mongos.hardware
  puts "start mongos at hardware:#{hardware}"
  cmd = "mongos --chunkSize #{CHUNK_SIZE} --port=#{port} --configdb #{IP_ADDRESS}:#{cluster.config.port}  > #{TEST_DIR}/mongos_#{port}.log"
  start_server(cmd,port)
end

sleep 10

# setup shardings
puts 'setting up shards'
cluster.shards.each do |shard|

  puts shard.shard_config_string
  loop do
    begin
      mongos = Mongo::Connection.new(IP_ADDRESS,cluster.mongos[0].port)
      puts mongos['admin'].command({'addshard'=>shard.shard_config_string})
      break
    rescue =>e
      sleep 1
      print '.'
      # HACKME
    end
  end
end

puts 'confirm shards'
mongos = Mongo::Connection.new(IP_ADDRESS,cluster.mongos[0].port)
mongos['config']['shards'].find().each{|shard|p shard}

puts mongos['admin'].command({'enablesharding'=>'hoge'})
mongos['config']['databases'].find().each{|shard|p shard}

#
# be careful to use incremental ids for shard key !!!(basicall you should not(hot spot problem))
#
puts mongos['admin'].command({'shardcollection'=>'hoge.users','key'=>{'user_id'=>1},'unique'=>true})

puts 'create test collection "hoge.users"'
(1..50000).each do |i|
  mongos['hoge']['users'].save({'user_id'=>i,'name'=>"user#{i}",'email'=>"test#{i}@example.com"})
end

puts mongos['config']['shards'].find().each{|shard|p shard}

puts <<"EOS"
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
creating test environment done
commands below are examples for testing
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

# starting servers again
#{@commands.join(" &\n") + " &"}

# for checking replicaset(access one of replica)
mongo #{IP_ADDRESS}:#{cluster.shards[0].replicaset.primary.port}
rs.status();

# for checking sharding(access one of mongos)
mongo #{IP_ADDRESS}:#{cluster.mongos[0].port}
use admin
db.printShardingStatus(true);
use hoge
db.users.count();
db.users.save({user_id:500001,name:'fuga'});
db.users.count();

# to use this script again(kill all test processes and erace all datas and run script again)
pkill -9 mongod && pkill -9 mongos ; rm -rf #{TEST_DIR}/* ; ruby mongo_test.rb

EOS

#loop do
  #puts "users.count="+mongos['hoge']['users'].size().to_s
  #sleep 1
#end
