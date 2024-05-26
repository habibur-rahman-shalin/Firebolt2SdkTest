using FireboltDotNetSdk.Client;
using System.Data;
using System.Data.Common;
using Firebolt2.Connection;

//starting call for first token
await RunFireboltQueryWithSDK(
            connectionString: ConnectionString.GetFirebolt2ConnectinoString(),
            query: "SELECT * FROM m_time_period_groups;",
            count: -1);

Console.WriteLine("First request Completed. No more Token Generation");

var tasks = new List<Task>();
for (int i = 0; i < 9; i++)
{
    tasks.Add(
        RunFireboltQueryWithSDK(
            connectionString: ConnectionString.GetFirebolt2ConnectinoString(),
            query: "SELECT * FROM m_time_period_groups;",
            count: i)
        );
}

await Task.WhenAll(tasks);

Console.WriteLine("All request Completed");

static async Task RunFireboltQueryWithSDK(string connectionString, string query, int count = 0)
{
    Console.WriteLine($"starting query {count}");
    try
    {
        using var conn = new FireboltConnection(connectionString);
        await conn.OpenAsync();

        using var command = conn.CreateCommand();
        command.CommandText = query;

        DbDataReader reader = command.ExecuteReader();
        DataTable table = new DataTable();
        table.Load(reader);

        DisplayDataTable(table);

        //var reader = await command.ExecuteReaderAsync();
        //var dt = ConvertDataReaderToDataTable(reader);

        conn.Close();
    }
    catch (Exception ex)
    {
        Console.WriteLine($"Error Generated {ex.Message} -> {ex.InnerException?.Message}");
    }
    Console.WriteLine($"ended query {count}");
}



static DataTable ConvertDataReaderToDataTable(DbDataReader reader)
{
    DataTable table = new DataTable();

    // Create columns based on the schema from the DataReader
    for (int i = 0; i < reader.FieldCount; i++)
    {
        table.Columns.Add(reader.GetName(i), reader.GetFieldType(i));
    }

    // Read data from the DataReader and populate the DataTable
    while (reader.Read())
    {
        DataRow row = table.NewRow();
        for (int i = 0; i < reader.FieldCount; i++)
        {
            row[i] = reader[i];
        }
        table.Rows.Add(row);
    }

    return table;
}

static void DisplayDataTable(DataTable table)
{
    // Removing all but the first three columns
    while (table.Columns.Count > 3)
    {
        table.Columns.RemoveAt(3);
    }

    // Write the DataTable to the console
    foreach (DataColumn column in table.Columns)
    {
        Console.Write("{0,-15} \t", column.ColumnName);
    }
    Console.WriteLine();

    foreach (DataRow row in table.Rows)
    {
        foreach (var item in row.ItemArray)
        {
            Console.Write("{0,-15} \t", item);
        }
        Console.WriteLine();
    }
}