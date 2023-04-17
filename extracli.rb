require "readline"
require "./my_sqlite_request.rb"

class MySqliteCli

        def bufferCut(buffer, value)
            return buffer.split(value)
        end

        def stripQuotes(value)
            val = value.chop
            val[0]=""
            return val
        end

        def call_where_command(arr)
            data = {}
            i = arr.index("WHERE")
            i += 1
            field =""
            if arr[i] != nil
            field = arr[i]
            end
            valueOfWhere = arr [i +2]
            valueOfWhere = stripQuotes(valueOfWhere)
            if valueOfWhere != nil
                data[field] = valueOfWhere
            end
            return data
        end


        def do_wheree(columnName)
        conditions = call_where_command(@requestArr)
        @field = conditions.keys[0]
        @value = conditions.values[0]
        @request = MySqliteRequest.new().from(@tableName).select(columnName).where(@field, @value).run()

        end

        def do_orderr (columnName)
            @i = @requestArr.index("BY") + 1
            colName = @requestArr[@i]
            order = @requestArr[@i + 1]
            @request =  MySqliteRequest.new().from(@tableName).select(columnName).order(order, colName).run()
        end

        def do_joinn (columnName)
            @i = @requestArr.index("JOIN") + 1
            joinedTable = @requestArr[@i]  + ".csv"
            @i = @requestArr.index("ON") + 1
            colDbA = @requestArr[@i].split(".")[1]
            colDbB = @requestArr[@i + 2].split(".")[1]
            
            @request =  MySqliteRequest.new().from(@tableName).select(columnName).join(colDbA, joinedTable, colDbB).run()
            
        end

        def call_select_command(buffer)
            @requestArr = bufferCut(buffer, " ")
            columnName = nil
            @tableName = nil
            @field = nil
            @value = nil
            @request = nil
            @i = 1

            columnName = @requestArr[@i]

            if columnName.include? ","
                columnName = columnName.split(",")
            end
            
            @i = @requestArr.index("FROM") + 1

            @tableName = @requestArr[@i] + ".csv"

            @i = @i + 1

            if @requestArr.include? "WHERE"
                do_wheree( columnName)
            elsif @requestArr.include? "JOIN"
                do_joinn( columnName)

            elsif @requestArr.include? "ORDER"
                do_orderr( columnName)
            else 
                @request = MySqliteRequest.new().from(@tableName).select(columnName).run()

            end

            print @request
            puts ""

        end

        def new_hash_store(tableName, data)
            hash = {}
            table = MySqliteRequest.new().read_File(tableName)
            heads = table[0].keys
            i=0
            heads.each do |head|
            hash.store(head, data[i])
            i +=1
            end
            return hash
        end

        def call_insert_command(buffer)
            requestArr = bufferCut(buffer, " ")
            tableName = requestArr[2] + ".csv"
            values = bufferCut(buffer, "(")
            data = values[1].chop.split(",")
            newData = new_hash_store(tableName, data)
            request = MySqliteRequest.new().insert(tableName).values(newData).run()

        end

        def call_update_command(buffer)
            requestArr = bufferCut(buffer, " ")
            i = 1
            tableName = requestArr[i] + ".csv"

            i = requestArr.index("SET") 
            i +=1
            data = {}

            if requestArr[i] != nil
                while requestArr[i] != "WHERE"

                    key = requestArr[i]
                    val = requestArr[i + 2]
                    newVal = val.split("").last 

                    if newVal== ","
                        val= val.chop
                    end

                    val = stripQuotes(val)
                    data[key] = val
                    i = i +3
                end
            end
            condition = call_where_command(requestArr)
            field = condition.keys[0]
            value = condition.values[0]

            request = MySqliteRequest.new().update(tableName).values(data).where(field, value).run

        end

        def call_delete_command(buffer)
            requestArr = bufferCut(buffer, " ")
            i = requestArr.index("DELETE") 
            i += 2

            tableName = requestArr[i] + ".csv"
            condition = call_where_command(requestArr)
        
            value = condition.values[0]  
            field = condition.keys[0]

            request = MySqliteRequest.new().delete().from(tableName).where(field, value).run()
        end


        def readReq(buffer)

        req = buffer.split.first
            if req == "SELECT"
                call_select_command(buffer)
            elsif req =="INSERT"
                call_insert_command(buffer)
            elsif req == "UPDATE"
                call_update_command(buffer)
            elsif req == "DELETE"
                call_delete_command(buffer) 
            else 
                print "wrong request! \n"
            end
        end


        def readInput
            while buffer = Readline.readline("my_sqlite_cli> ", true)
                if buffer == "quit"
                    break
                end

                readReq(buffer)
            end
            return nil
        end

end

run = MySqliteCli.new
run.readInput


#SELECT * FROM students

#SELECT name,email FROM students WHERE name = 'Mila'

#INSERT INTO students VALUES (John,john@johndoe.com,A,https://blog.johndoe.com)

#UPDATE students SET email = 'jane@janedoe.com', blog = 'https://blog.janedoe.com' WHERE name = 'Mila'

#DELETE FROM students WHERE name = 'John'