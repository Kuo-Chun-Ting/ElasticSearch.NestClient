using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace ElasticSearch.NestClient.TestConsole
{
    class Program
    {
        static void Main(string[] args)
        {
            NestClient client = new NestClient("http://192.168.8.151:9200");

            string type = "ncree_receiver_station_raw";
            string field = "PGA";
            int lowerBoound = 1;

            Dictionary<string, string[]> fields = new Dictionary<string, string[]>();
            fields.Add("TownshipCode", new string[1] { "1000508" });

            DateTime startTime = new DateTime(2019, 11, 20);
            DateTime endTime = new DateTime(2019, 11, 29);

            var result = client.SearchByMatchTermsGreaterThanAsync<JObject>(type, startTime, endTime, field, lowerBoound, fields).Result;
        }
    }
}
