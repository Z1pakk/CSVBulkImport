using System;
using System.Data.SqlClient;
using System.IO;
using Microsoft.Extensions.Configuration;

namespace CSVBulkExport
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Operation started");
            
            var configuration =  new ConfigurationBuilder()
                .AddJsonFile($"appsettings.json");
            
            var config = configuration.Build();
            var connectionString = config.GetSection("ConnectionString").Value;
            
            using (SqlConnection con = new SqlConnection(connectionString))
            {
                con.Open();
                
                Console.WriteLine("Bulk insertion started");

                // Bulk insert
                SqlCommand bulkInsertCommand =
                    new SqlCommand(@$"
                        BULK INSERT dbo.Data 
                        FROM '{Path.GetFullPath("sample-cab-data.csv")}' 
                        WITH (
                            FIELDTERMINATOR = ',', 
                            ROWTERMINATOR = '\n',
                            FIRSTROW = 2,
                            FORMATFILE ='{Path.GetFullPath("csv-format.fmt")}'
                            )
                    ", con);

                bulkInsertCommand.ExecuteNonQuery();
                
                Console.WriteLine("Bulk insertion has been completed");
                
                con.Close();
                
                // Find duplications
                SqlCommand findDuplicationCommands = new SqlCommand(@$" SELECT * FROM SelectDuplicatesView", con);

                using (StreamWriter csvWriter = new StreamWriter(File.Open(Path.Combine(Path.GetFullPath("./"), "duplicates.csv"), FileMode.Create)))
                {
                    csvWriter.WriteLine("tpep_pickup_datetime," +
                                        "tpep_dropoff_datetime," +
                                        "passenger_count," +
                                        "trip_distance," +
                                        "store_and_fwd_flag," +
                                        "PULocationID," +
                                        "DOLocationID," +
                                        "fare_amount," +
                                        "tip_amount");
                    
                    con.Open();
                    using (SqlDataReader reader = findDuplicationCommands.ExecuteReader())
                    {
                        if (reader.HasRows)
                        {
                            while (reader.Read())
                            {
                                csvWriter.WriteLine($"{reader["tpep_pickup_datetime"]}," +
                                                    $"{reader["tpep_dropoff_datetime"]}," +
                                                    $"{reader["passenger_count"]}," +
                                                    $"{reader["trip_distance"]}," +
                                                    $"{reader["store_and_fwd_flag"]}," +
                                                    $"{reader["PULocationID"]}," +
                                                    $"{reader["DOLocationID"]}," +
                                                    $"{reader["fare_amount"]}," +
                                                    $"{reader["tip_amount"]}");
                            }
                        }
                    }
                    
                    con.Close();
                }
                
                con.Open();
                
                SqlCommand deleteDuplicatedRecordCommand = new SqlCommand(@$"EXEC DeleteDuplicatedRecords", con);
                deleteDuplicatedRecordCommand.ExecuteNonQuery();
                
                Console.WriteLine("Duplicated records have been removed and stored in the csv file");
                
                SqlCommand updateFlagsCommand = new SqlCommand(@$"EXEC UpdateFlags", con);
                updateFlagsCommand.ExecuteNonQuery();
                
                Console.WriteLine("Flags have been updated");

                SqlCommand updateDatesCommand = new SqlCommand($@"EXEC ConvertDatesToUTC", con);
                updateDatesCommand.ExecuteNonQuery();
                
                Console.WriteLine("Dates have been completed");
                
                con.Close();
            }
            
            Console.WriteLine("Operation successfully completed");

        }
    }
}
