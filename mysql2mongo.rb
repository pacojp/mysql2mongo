# -*- coding: utf-8 -*-
#
# this program uses mysql library instead of mysql2 library because of 'server side cursor problem(for too-big-tables)'. 
# under my circumstans, mysql2 :stream option does not work well(may be database is too old(mysql 4.1.***))(2010/4/20).
#
# !! may be mysql library version should be 2.8.XXX. it seems that 3.XXX(still alpla version now) will not support server side cursor.
#
# HACKME
# maybe column name 'id' should set to be '_id'
#

require 'rubygems'
require 'mysql' # should be 2.8.XXX # should use bundler......sorry
require 'mongo'

MONGO_SETTING = {
  :connection_string => 'localhost',
  :database => 'test'
}

MYSQL_SETTING = {
  :host=>'localhost',
  :user=>'hoge',
  :password=>'hoge',
  :database=>'test',
  :charset_name=>'utf8'
}

my = Mysql.init
my.options(Mysql::SET_CHARSET_NAME, MYSQL_SETTING[:charset_name])
my.connect(MYSQL_SETTING[:host],MYSQL_SETTING[:user],MYSQL_SETTING[:password],MYSQL_SETTING[:database])
my.query_with_result = false

mongo = Mongo::Connection.new(MONGO_SETTING[:connection_string])
mongo_db = mongo[MONGO_SETTING[:database]]

tables = []
query = "show tables"
my.query(query)
my.use_result.each do |row|
  tables << row[0]
end

tables.each_with_index do |current_table,i|
  puts "-- #{current_table} --"
  coll = mongo_db[current_table]
  query = "select * from #{current_table}"
  my.query(query)
  j = 0
  results = my.use_result
  results.each_hash do |hash|
    print "." if j % 10000 == 0
    coll.insert(hash)
    j += 1
  end
  puts ''
  #resutl = nil
end
