using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using FireboltDoNetSdk;
using FireboltDotNetSdk.Client;
using Firebolt2.Connection;

namespace Firebolt2
{
    class Program
    {
        static async Task Main(string[] args)
        {
            using FireboltConnection conn = ConnectBySdkPhase2DevAccount();
            //using FireboltConnection conn = ConnectBySdkDevAccount();
            //await ConnectByAPI();
        }

        private static FireboltConnection ConnectBySdkPhase2DevAccount()
        {

            string conn_string = ConnectionString.GetFirebolt2ConnectinoString();
            var conn = new FireboltConnection(conn_string);
            // Open a connection
            conn.Open();

            // First you would need to create a command
            var command = conn.CreateCommand();

            // ... and set the SQL query
            command.CommandText = "SELECT * FROM m_time_period_groups;";

            // Execute a SQL query and get a DB reader
            DbDataReader reader = command.ExecuteReader();

            // Optionally you can check whether the result set has rows
            Console.WriteLine($"Has rows: {reader.HasRows}");

            // Close the connection after all operations are done
            conn.Close();
            return conn;
        }

        private static async Task ConnectByAPI()
        {
            var httpClient = new HttpClient();

            string baseUrl = $"https://api.app.firebolt.io";
            string clientId = "Miv4XJJOgHLtq7h5FNKepdLuaIkTPstH";
            string clientSecret = "40wY3wpEYQ4vGoNlspjnNcx2pm2r7Ezt4hRqF3vQwOAWSEjAtPD7tZjgYS_Iyc9L";
            string accountName = "dev";
            string getEngineUrl = $"pmi-data-qa-team-es-dsv2-general-purpose.01hdqzwcpy0a9rp9s42stnpc7a.us-east-1.app.firebolt.io";
            string databaseName = "pmi-data-qa-team-es-dsv2";
            string query = "SELECT * from M_DATA_SOURCES";

            await ExecuteQuery2();

            var tokenInfo = await GetFireboltToken(httpClient, clientId, clientSecret);

            if (tokenInfo != null)
            {
                var engine_url = getEngineUrl; //await GetEngineUrl(httpClient, getEngineUrl, tokenInfo);               

                if (engine_url != null)
                {
                    await ExecuteQuery(httpClient, databaseName, query, engine_url, tokenInfo);
                }
            }
        }

        private static FireboltConnection ConnectBySdkDevAccount()
        {
            string account = "dev";
            string clientId = "iZrJcWEpHYxilSglDiaRZo6EG7bAe35k";
            string clientSecret = "f0fs9mKJh3jDjaEY4AAl2BuGwfMDi3Iv46QAF29uSjn5l2nNQ7SARmhY4dHRDnXE";
            string database = "pmi_data_qa_team_es_dsv2";
            string engine = "pmi_data_qa_team_es_dsv2_general_purpose";
            string conn_string = $"account={account};clientid={clientId};clientsecret={clientSecret};database={database};engine={engine};env=app";
            var conn = new FireboltConnection(conn_string);
            // Open a connection
            conn.Open();

            // First you would need to create a command
            var command = conn.CreateCommand();

            // ... and set the SQL query
            command.CommandText = "SELECT * from M_DATA_SOURCES";

            // Execute a SQL query and get a DB reader
            DbDataReader reader = command.ExecuteReader();

            // Optionally you can check whether the result set has rows
            Console.WriteLine($"Has rows: {reader.HasRows}");

            // Close the connection after all operations are done
            conn.Close();
            return conn;
        }

        private static async Task ExecuteQuery(HttpClient httpClient, string databaseName, string query, string engine_url, string tokenInfo)
        {
            var tokenResponse = JsonSerializer.Deserialize<TokenResponse>(tokenInfo);

            httpClient.DefaultRequestHeaders.Add("Accept", "application/json");
            httpClient.DefaultRequestHeaders.Add("Authorization", $"Bearer {tokenResponse?.AccessToken}");
            httpClient.DefaultRequestHeaders.TryAddWithoutValidation("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36 Edge/16.16299 .NET CLR 3.5.30729 .NET CLR 3.0.30729 .NET CLR 2.0.50727");

            string queryExecutionUrl = $"https://{engine_url}/query?database={databaseName}";
            string apiUrl = "https://pmi-data-qa-team-es-dsv2-general-purpose.01hdqzwcpy0a9rp9s42stnpc7a.us-east-1.app.firebolt.io/query?database=pmi_data_qa_team_es_dsv2";

            try
            {
                //httpClient.DefaultRequestHeaders.Add("Authorization", $"Bearer {tokenResponse?.AccessToken}");
                var response = await httpClient.PostAsync(queryExecutionUrl, new StringContent(query));
                response.EnsureSuccessStatusCode();

                var responseBody = await response.Content.ReadAsStringAsync();
                Console.WriteLine(responseBody);


                // Your SQL query
                var query2 = new { query = "SELECT * from M_DATA_SOURCES" };

                // Convert query to JSON string
                var jsonQuery = Newtonsoft.Json.JsonConvert.SerializeObject(query2);

                // Create HttpContent for the query
                var content = new StringContent(jsonQuery, System.Text.Encoding.UTF8, "application/json");

                // Send POST request to the API endpoint
                var response2 = await httpClient.PostAsync(queryExecutionUrl, content);

                // Read response content
                string responseBody2 = await response2.Content.ReadAsStringAsync();
            }
            catch (HttpRequestException e)
            {
                Console.WriteLine($"HTTP Request Error: {e.Message}");
            }
        }

        static async Task ExecuteQuery2()
        {

            var client = new HttpClient();
            var request = new HttpRequestMessage(HttpMethod.Post, "https://pmi-data-qa-team-es-dsv2-general-purpose.01hdqzwcpy0a9rp9s42stnpc7a.us-east-1.app.firebolt.io/query?database=pmi_data_qa_team_es_dsv2");
            request.Headers.Add("Authorization", "Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6ImxCVmVOZzVlcnRhcVFJVzBmMlp6QSJ9.eyJodHRwczovL2ZpcmVib2x0LmlvL2NsYWltcyI6eyJnbG9iYWxfcm9sZXMiOlsiT1JHQU5JWkFUSU9OX0FETUlOIl0sImlkIjoiNTM4NDEwMzgtMWUwMi00YTE2LTk5NjAtZWQ2ZTIwY2M4NzQ4Iiwib3JnYW5pemF0aW9uX2lkIjoiMmQwNDljMmMtZjgxYS00NDBmLWJkOWEtYjYyYzJmZTUyZDA4Iiwib3JnYW5pemF0aW9uX25hbWUiOiJpcXZpYSIsInNlcnZpY2VfYWNjb3VudF9pZCI6IjcxY2QxNWM1LWJlNTMtNGQ3YS1hMTI0LTZlMGZkYmE3NzA3OCJ9LCJpc3MiOiJodHRwczovL2lkLmFwcC5maXJlYm9sdC5pby8iLCJzdWIiOiJNaXY0WEpKT2dITHRxN2g1Rk5LZXBkTHVhSWtUUHN0SEBjbGllbnRzIiwiYXVkIjoiaHR0cHM6Ly9hcGkuZmlyZWJvbHQuaW8iLCJpYXQiOjE3MTU1OTYwNjIsImV4cCI6MTcxNTYwMzI2Miwic2NvcGUiOiJzZXJ2aWNlLWFjY291bnQiLCJndHkiOiJjbGllbnQtY3JlZGVudGlhbHMiLCJhenAiOiJNaXY0WEpKT2dITHRxN2g1Rk5LZXBkTHVhSWtUUHN0SCJ9.Zei75w8VCBvXMEakLHU8WbaZ90p3H8VTqdoXBGoBcfpv_njH9IglDLZVHEhCdAOXNByOvS-7f8Oi73kkt8i_D21yxjQMLQEE2xGJaJCYUzaRk9XrlRKX969tDW7RTpQg7C588r0Xl70Mp4IHUdZHuydYx9-rg6BwzIYuFDEBQZCJ7J5R6xUUL2ANqqTFamSg5vzTcb87ec-qmZ-LnuUjKngNkd_20Ggt5sehV0txm39yccQqemIq1VVEMYw2K5WauAP8wQlLe-CIVrU0EHECcHE-v7voAUgE5c9YYCbVmsYbi-AB6j0Bj2k0N4_sTWRyw74EtSjVtt3MkRit5hAkzw");
            client.DefaultRequestHeaders.TryAddWithoutValidation("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36 Edge/16.16299 .NET CLR 3.5.30729 .NET CLR 3.0.30729 .NET CLR 2.0.50727");

            //request.Headers.Add("Cookie", "AWSALB=fihVGNnOgEpKWmHAo4aMT5urhmr2YNg6lJJckXbHlPbsVYxin/F7+vVLwM0j1NMnG1MTOnVmNTkyAwMPXEcXNzZWnwC0nK4gtPrL+lZDxE/7CTajksqPNJJGq6Bo; AWSALBCORS=fihVGNnOgEpKWmHAo4aMT5urhmr2YNg6lJJckXbHlPbsVYxin/F7+vVLwM0j1NMnG1MTOnVmNTkyAwMPXEcXNzZWnwC0nK4gtPrL+lZDxE/7CTajksqPNJJGq6Bo");
            var content = new MultipartFormDataContent();
            content.Add(new StringContent("SELECT * from M_DATA_SOURCES"), "query");
            request.Content = content;
            var response = await client.SendAsync(request);
            response.EnsureSuccessStatusCode();
            Console.WriteLine(await response.Content.ReadAsStringAsync());

            //string apiUrl = "https://pmi-data-qa-team-es-dsv2-general-purpose.01hdqzwcpy0a9rp9s42stnpc7a.us-east-1.app.firebolt.io/query?database=pmi_data_qa_team_es_dsv2";
            //string bearerToken = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6ImxCVmVOZzVlcnRhcVFJVzBmMlp6QSJ9.eyJodHRwczovL2ZpcmVib2x0LmlvL2NsYWltcyI6eyJnbG9iYWxfcm9sZXMiOlsiT1JHQU5JWkFUSU9OX0FETUlOIl0sImlkIjoiM2EzZGZkYWMtZTc1OC00NzhjLWE3NmYtZGVhYTFjYWZkMTk3Iiwib3JnYW5pemF0aW9uX2lkIjoiMmQwNDljMmMtZjgxYS00NDBmLWJkOWEtYjYyYzJmZTUyZDA4Iiwib3JnYW5pemF0aW9uX25hbWUiOiJpcXZpYSIsInNlcnZpY2VfYWNjb3VudF9pZCI6IjcxY2QxNWM1LWJlNTMtNGQ3YS1hMTI0LTZlMGZkYmE3NzA3OCJ9LCJpc3MiOiJodHRwczovL2lkLmFwcC5maXJlYm9sdC5pby8iLCJzdWIiOiJNaXY0WEpKT2dITHRxN2g1Rk5LZXBkTHVhSWtUUHN0SEBjbGllbnRzIiwiYXVkIjoiaHR0cHM6Ly9hcGkuZmlyZWJvbHQuaW8iLCJpYXQiOjE3MTU1ODcyNjAsImV4cCI6MTcxNTU5NDQ2MCwic2NvcGUiOiJzZXJ2aWNlLWFjY291bnQiLCJndHkiOiJjbGllbnQtY3JlZGVudGlhbHMiLCJhenAiOiJNaXY0WEpKT2dITHRxN2g1Rk5LZXBkTHVhSWtUUHN0SCJ9.paG4S1_55YKc-Jt0KmlLWlwOPBsPaVveuU2UoCj1elqad_KsgDcBVbv82lDcnFRKKvJzniRtoMaEg2XLcxLF7ZlbCFe-AhL5aKc34YmfPlKN4IxgDtqZTn0G_cKaAN3YXo0lwVJKzJ298MzQ2kGr3STuFPk16rVZyo1PlRKAUYNuD9cRoijnNkGPuDaMjKdIlyUic8y414cAqcG0enae06_4XtVK7TwMUusMduVbI-sPZ1AeHpcDiIwupnx2iCr2zm_xfk-_OA3SDgwf61DMgw0bjoCWyTxLetox_tvZHW3ARfcEziCPusxYDOx4al17ukgzup5W43y8E3vc2i_fbQ"; // Replace with your actual bearer token

            //// Create HttpClientHandler
            //var handler = new HttpClientHandler();

            //// Configure SSL/TLS settings
            //handler.SslProtocols = System.Security.Authentication.SslProtocols.Tls12;
            //handler.ServerCertificateCustomValidationCallback = (sender, certificate, chain, sslPolicyErrors) => true;
            //handler.CheckCertificateRevocationList = false;

            //using (var httpClient = new HttpClient(handler))
            //{
            //    // Add authorization header
            //    httpClient.DefaultRequestHeaders.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", bearerToken);

            //    // Add content type header
            //    httpClient.DefaultRequestHeaders.Accept.Add(new System.Net.Http.Headers.MediaTypeWithQualityHeaderValue("application/json"));

            //    // Your SQL query
            //    var query = new { query = "SELECT * from M_DATA_SOURCES" };

            //    // Convert query to JSON string
            //    var jsonQuery = Newtonsoft.Json.JsonConvert.SerializeObject(query);

            //    // Create HttpContent for the query
            //    var content = new StringContent(jsonQuery, System.Text.Encoding.UTF8, "application/json");

            //    // Print request details
            //    Console.WriteLine("Request Headers:");
            //    foreach (var header in httpClient.DefaultRequestHeaders)
            //    {
            //        Console.WriteLine($"{header.Key}: {string.Join(", ", header.Value)}");
            //    }
            //    Console.WriteLine("Request Body:");
            //    Console.WriteLine(jsonQuery);

            //    // Send POST request to the API endpoint
            //    var response = await httpClient.PostAsync(apiUrl, content);

            //    // Read response content
            //    string responseBody = await response.Content.ReadAsStringAsync();

            //    // Print response details
            //    Console.WriteLine("Response Headers:");
            //    foreach (var header in response.Headers)
            //    {
            //        Console.WriteLine($"{header.Key}: {string.Join(", ", header.Value)}");
            //    }
            //    Console.WriteLine("Response Body:");
            //    Console.WriteLine(responseBody);
            //}
        }

        private static async Task<string?> GetEngineUrl(HttpClient httpClient, string getEngineUrl, string tokenInfo)
        {
            var tokenResponse = JsonSerializer.Deserialize<TokenResponse>(tokenInfo);

            //httpClient.DefaultRequestHeaders.Add("Accept", "application/json");
            //httpClient.DefaultRequestHeaders.Add("Authorization", $"Bearer {tokenResponse?.AccessToken}");

            try
            {
                var engineUrlResponse = await httpClient.GetAsync(getEngineUrl);
                engineUrlResponse.EnsureSuccessStatusCode();

                var engineUrlRsponseInfo = await engineUrlResponse.Content.ReadAsStringAsync();
                Console.WriteLine(engineUrlRsponseInfo);

                var engineInfo = JsonSerializer.Deserialize<Engine>(engineUrlRsponseInfo);
                return engineInfo?.EngineUrl;
            }
            catch (HttpRequestException e)
            {
                Console.WriteLine($"HTTP Request Error: {e.Message}");
            }

            return string.Empty;
        }

        private static async Task<string> GetFireboltToken(HttpClient httpClient, string client_id, string client_secret)
        {
            var requestBody = new FormUrlEncodedContent(new[]
                        {
            new KeyValuePair<string, string>("client_id", client_id),
            new KeyValuePair<string, string>("client_secret", client_secret),
            new KeyValuePair<string, string>("grant_type", "client_credentials"),
            new KeyValuePair<string, string>("audience", "https://api.firebolt.io")
        });

            var response = await httpClient.PostAsync("https://id.app.firebolt.io/oauth/token", requestBody);
            var content = await response.Content.ReadAsStringAsync();

            Console.WriteLine(content);
            return content;
        }
    }
}
