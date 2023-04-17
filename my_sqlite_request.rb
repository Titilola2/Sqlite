require 'csv'

class MySqliteRequest
  def initialize
    @ans = []
    @tableHeads = []
    @holder = '' 
  end

  def read_File(sqlite_csv_file)
  #This function reads the headers in the csv file passed and passes it to a hash and stores it there.
    table = nil
    if sqlite_csv_file
       table = CSV.foreach(sqlite_csv_file, headers: true).map { |row| row.to_h }
     else
       print 'No such file'
     return nil
    end
    table
  end    

  def insert_method(hash_list, new_hash)
    hash_list.concat(new_hash.is_a?(Array) ? new_hash : [new_hash])
  end

  def hash_to_file(hash_list, db_name)
    return if hash_list.empty?
    CSV.open(db_name, 'w', write_headers: true, headers: hash_list.first.keys) do |csv|
      hash_list.each { |hash| csv << hash.values }
    end
  end
      
  def from(sqlite_csv_file)
    @table = read_File(sqlite_csv_file)
    @tableHeads << @table[0].keys
    @ans = @table
    @ans
    self
  end
      
  def insert(sqlite_csv_file)
    @holder = 'insert'
    @sqlite_csv_file = sqlite_csv_file
    @ans = read_File(sqlite_csv_file)
     self
  end

  def where(column_name, criteria)
    case @holder 
    when 'select'
      if @column_name.class == Array
        # puts @column_name
       whereArr = []
       whereArr = @table.select {|hash| hash[@column_name[0]] == criteria}.map {|hash| hash.values_at(*@column_name)}
       result = whereArr.each_slice(2).map do |a, b| { @column_name[0] => a, @column_name[1] => b }
       end
       @ans = result
      else
        @ans = @table.select {|hash| hash[column_name] == criteria}.map {|hash| { @column_name => hash[@column_name] } }
      end

    when 'update'
      return self unless column_name && criteria
      @ans.each do |hash|
        if hash[column_name] == criteria
          @newData.each { |key, val| hash[key] = val }
        end
      end
      hash_to_file(@ans, @file_name)

    when 'delete'
      index = nil
      @ans.each do |hash|
        if hash[column_name] == criteria
          index = @ans.index(hash)
          break # exit loop after first match is found
        end
      end
      if index
        @ans.delete_at(index)
        def delete_from_file(csvName, column_name, criteria)
          rows = CSV.read(CsvName, headers: true)
          rows.delete_if { |row| row[column_name] == criteria }
          CSV.open(CsvName, 'w', headers: true) { |csv| csv << rows.headers; rows.each { |row| csv << row } }
        end
      end
    end
   self
  end

  def select(column_name)
    @column_name = column_name
    @holder = 'select'
   if column_name == '*'
    @ans = @table
   else
    @ans = @table.map { |hash| hash.select { |key, _| column_name.is_a?(Array) ? column_name.include?(key) : key == column_name } }
   end
    self
  end

  def values(data)
    @data = data
   if @holder == 'insert'
   insert_method(@ans, @data)
    hash_to_file(@ans, @sqlite_csv_file)
   elsif @holder == 'update'
    @newData = @data
   end
  self
  end
 
  def update(file_name)
    @file_name, @holder, @ans = file_name, 'update', read_File(file_name)
    self
  end
  
  def delete
    @holder = 'delete'
    self
  end

    def run
     puts @ans
    end
end


