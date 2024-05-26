namespace Firebolt2.Connection
{
    public class ConnectionString
    {
        public static string GetFirebolt2ConnectinoString()
        {
            string account = "Phase2-dev";
            string clientId = "SXGWxkbfAaTftsS6wUfVlD4usBHnOmEt";
            string clientSecret = "rOmsxUtQc0MrCSpTIo3-cOhbSEigm9QQTPXTrXypwpARssDnAU-8SJXlKRcrUdmN";
            //string clientId = "iZrJcWEpHYxilSglDiaRZo6EG7bAe35k";
            //string clientSecret = "f0fs9mKJh3jDjaEY4AAl2BuGwfMDi3Iv46QAF29uSjn5l2nNQ7SARmhY4dHRDnXE";

            string database = "pmi_data_qa_team_es_dsv2";
            string engine = "pmi_data_qa_team_es_dsv2_stacked_general_purpose";
            return $"account={account};clientid={clientId};clientsecret={clientSecret};database={database};engine={engine}";

        }

    }
}