require 'mysql2/em'
require 'eventmachine'
require 'dotenv/load'
require 'pry'
require_relative 'lib/transformer'

def partition(size)
  case size
  when 1..500
    1
  when 501..1000
    2
  when 1001..1500
    3
  when 1501..2500
    4
  when 2501..3000
    5
  when 3001..3500
    6
  when 3500..4000
    7
  else
    0
  end
end

update_query = ""

client = Mysql2::Client.new(
  host: ENV['HOST'],
  database: ENV['DATABASE'],
  username: ENV['USERNAME'],
  password: ENV['PASSWORD']
)
client.query("delete from hle_dev_test_osolyannik where candidate_office_name = '';")
count_result = client.query("select count(id) from hle_dev_test_osolyannik a;")
table_count = count_result.first['count(id)']
result = partition(table_count)
limit = (table_count / result).ceil
table = client.query("select id, candidate_office_name from hle_dev_test_osolyannik a;", :cast => false)

EM.run do
  table.each_with_index do |row, i|
    id = row['id']
    name = row['candidate_office_name']

    clean_name = Transformer.new(name).perform
    sentence = "Candidate is running for the #{clean_name} office"
    update_query += "update hle_dev_test_osolyannik set clean_name=\"%s\", sentence=\"%s\" where id = %s;\n" % [clean_name, sentence, id.to_s]

    if i % limit > limit - 2
      updating_client = Mysql2::EM::Client.new(
        host: ENV['HOST'],
        database: ENV['DATABASE'],
        username: ENV['USERNAME'],
        password: ENV['PASSWORD'],
        flags: Mysql2::Client::MULTI_STATEMENTS
      )
      defer = updating_client.query(update_query, :async => true)
      defer.callback do |result|
        puts "Result of #{i} statements: #{result.to_a.inspect}"
        updating_client.close
      end
      update_query = ''
    end
  end

  unless update_query.empty?
    updating_client = Mysql2::EM::Client.new(
      host: ENV['HOST'],
      database: ENV['DATABASE'],
      username: ENV['USERNAME'],
      password: ENV['PASSWORD'],
      flags: Mysql2::Client::MULTI_STATEMENTS
    )
    defer = updating_client.query(update_query, :async => true)
    defer.callback do |result|
      puts "Last statements: #{result.to_a.inspect}"
      updating_client.close
    end
    update_query = ''
    EventMachine::stop_event_loop
  else
    EventMachine::stop_event_loop
  end
end

puts "Updated successfully!"
