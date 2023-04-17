require "readline"
require "./my_sqlite_request.rb"

class MySqliteCli
  def bufferCut(buffer, value)
    return buffer.split(value)
  end

  def chomp(value)
    val = value.chop
    val[0]=""
    return val
  end

  def call_command(arr)
    i = arr.index("WHERE").to_i + 1
    field = arr[i] || ""
    value = chomp(arr[i+2])
    { field => value } if value
  end


  def do_wheree(columnName)
    @field, @value = call_command(@requestArr).to_a.flatten
    @request = MySqliteRequest.new.from(@tableName).select(columnName).where(@field, @value).run
  end
  
  def do_orderr(columnName)
    i = @requestArr.index("BY").to_i + 1
    colName, order = @requestArr[i..i+1]
    @request = MySqliteRequest.new.from(@tableName).select(columnName).order(order, colName).run
  end
  
  def do_joinn(columnName)
    i = @requestArr.index("JOIN").to_i + 1
    joinedTable = @requestArr[i] + ".csv"
    i = @requestArr.index("ON").to_i + 1
    colDbA, colDbB = @requestArr.values_at(i, i+2).map { |s| s.split(".").last }
    @request = MySqliteRequest.new.from(@tableName).select(columnName).join(colDbA, joinedTable, colDbB).run
  end
  

  def call_select_command(buffer)
    @requestArr = bufferCut(buffer, " ")
    columnName = @requestArr[1]
    columnName = columnName.split(",") if columnName.include?(",")
    @tableName = @requestArr[@requestArr.index("FROM").to_i + 1] + ".csv"
    case @requestArr.find { |w| ["WHERE", "JOIN", "ORDER"].include?(w) }
    when "WHERE" then do_wheree(columnName)
    when "JOIN" then do_joinn(columnName)
    when "ORDER" then do_orderr(columnName)
    else @request = MySqliteRequest.new.from(@tableName).select(columnName).run
    end
    puts @request
  end

  def new_hash_store(tableName, data)
    table = MySqliteRequest.new.read_File(tableName)
    heads = table.first.keys
    Hash[heads.zip(data)]
  end
  
  def call_insert_command(buffer)
    requestArr = bufferCut(buffer, " ")
    tableName = requestArr[2] + ".csv"
    values = bufferCut(buffer, "(")
    data = values[1].chop.split(",")
    newData = new_hash_store(tableName, data)
    MySqliteRequest.new.insert(tableName).values(newData).run
  end
  
  def call_update_command(buffer)
    requestArr = bufferCut(buffer, " ")
    tableName = requestArr[1] + ".csv"
    i = requestArr.index("SET") + 1
    data = {}
    
    while requestArr[i] != "WHERE"
      key = requestArr[i]
      val = chomp(requestArr[i + 2])
      data[key] = val
      i += 3
    end
    
    condition = call_command(requestArr)
    field = condition.keys[0]
    value = condition.values[0]           
    MySqliteRequest.new.update(tableName).values(data).where(field, value).run
  end
  

  def call_delete_command(buffer)
    tableName = buffer.split[2] + ".csv"
    condition = call_command(buffer.split)
    field, value = condition.keys[0], condition.values[0]
    request = MySqliteRequest.new().delete().from(tableName).where(field, value).run()
  end
  

  def read_request_from_commandline(buffer)
    case buffer.split.first
    when "SELECT"
      call_select_command(buffer)
    when "INSERT"
      call_insert_command(buffer)
    when "UPDATE"
      call_update_command(buffer)
    when "DELETE"
      call_delete_command(buffer) 
    else 
      print "wrong request! \n"
    end
  end
  
  def read_input_command
    while buffer = Readline.readline("my_sqlite_cli> ", true)
        if buffer == "quit"
            break
        end
        read_request_from_commandline(buffer)
    end
    return nil
  end

end

run = MySqliteCli.new
run.read_input_command


#SELECT * FROM students

#SELECT name,email FROM students WHERE name = 'Mila'

#INSERT INTO students VALUES (John,john@johndoe.com,A,https://blog.johndoe.com)

#UPDATE students SET email = 'jane@janedoe.com', blog = 'https://blog.janedoe.com' WHERE name = 'Mila'

#DELETE FROM students WHERE name = 'John'