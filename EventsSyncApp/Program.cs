using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO.Compression;
using System.IO;
using Newtonsoft.Json;
using Npgsql;
using System.Configuration;
using System.Linq;
using Common.Util;

using Common.Util.Sql;
using Common.Util.Bags;
using Amazon.S3;
using Amazon.S3.Model;
using System.Net;

namespace EventsSyncApp
{
    internal class Program
    {
        private const string CompressedFileName = "../../archive.gz";
        public static NameValueCollection ConnectionSettings =
            (NameValueCollection)ConfigurationManager.GetSection("ConnectionSettings");

        static void Main(string[] args)
        {
            WriteAllOfBucket();
            
            Console.ReadKey();
        }
        
        private static void PostValue(InfobipEvent infobipEvent, string propsJ)
        {
            PGSqlUtil.AutoCreateTable(ConnectionSettings["eventTableName"], ConnectionSettings["eventTableSchema"], ConnectionSettings["connectString"]);
            PGSqlUtil.ExecuteNonQuery(new NpgsqlCommand($"INSERT INTO {ConnectionSettings["dbName"]} (eventid, eventname, userversion, personid, createdate, properties) " +
                                                        $"VALUES ('{infobipEvent.eventId}', '{infobipEvent.definitionId}', {Convert.ToInt32(infobipEvent.externalPersonId)}, " +
                                                        $"{Convert.ToInt32(infobipEvent.personId)}, '{infobipEvent.occurredTime}', '{propsJ}')"), ConnectionSettings["connectString"]);
        }

        static private async void WriteAllOfBucket()
        {
            var s3Client = new AmazonS3Client();
            var request = new ListObjectsV2Request
            {
                BucketName = ConnectionSettings["dctName"],
                MaxKeys = 5,
            };
            
            string timeLastDamp = GetTimeLastReading();
            var response = new ListObjectsV2Response();
            do
            {
                response = await s3Client.ListObjectsV2Async(request);
                var newDumps = response.S3Objects.FindAll(obj => obj.LastModified > Convert.ToDateTime(timeLastDamp));
                //response.S3Objects.ForEach(obj => {Console.WriteLine($"{obj.LastModified,10}\t{obj.Key,-35}\t{obj.Size,10}");});
                //newDumps.ForEach(obj => { Console.WriteLine($"{obj.LastModified,10}\t{obj.Key,-35}\t{obj.Size,10}"); });

                newDumps.ForEach(obj => ReadAllOfArchive($"{obj.Key,-35}"));
                newDumps.ForEach(obj => WriteReadingLogs(obj.Key, obj.LastModified.ToString()));

                request.ContinuationToken = response.NextContinuationToken;
            } 
            while (response.IsTruncated);
        }

        static private async void ReadAllOfArchive(string fileName)
        {
            if(fileName.IndexOf(".gz") == -1)
                return;
            var s3Client = new AmazonS3Client();
            var bucketName = ConnectionSettings["dctName"];
            var fRequest = new GetObjectRequest
            {
                BucketName = bucketName,
                Key = fileName
            };
            GetObjectResponse fResponse = await s3Client.GetObjectAsync(fRequest);
            if (fResponse.HttpStatusCode == HttpStatusCode.OK)
            {
                using (var rs = fResponse.ResponseStream)
                {
                    using (var dcmpr = new GZipStream(rs, CompressionMode.Decompress))
                    {
                        using (StreamReader sr = new StreamReader(dcmpr))
                        {
                            InfobipEvent infobipEvent = new InfobipEvent();
                            while (!sr.EndOfStream)
                            {
                                string json = sr.ReadLine();
                                var allVars = JSer.Deserialize<Bag>(json);
                                var columns = new string[]
                                {
                                    "eventId", "personId", "externalPersonId", "occurredTime", "definitionId",
                                    "sessionId"
                                };

                                var reservedFields = allVars.Clip(columns);
                                infobipEvent =
                                    JsonConvert.DeserializeObject<InfobipEvent>(JSer.Serialize(reservedFields,
                                        new { QuoteName = true }));

                                infobipEvent.Properties = allVars.RemoveAll(columns);
                                var propsJSON = JSer.Serialize(infobipEvent.Properties, new { QuoteName = true });

                                PostValue(infobipEvent, propsJSON);
                            }
                        }
                    }
                }
            }
        }

        static private void WriteReadingLogs(string name, string date)
        {
            if (name.IndexOf(".gz") == -1)
                return;
            DateTime dt = DateTime.Now;
            PGSqlUtil.AutoCreateTable(ConnectionSettings["readingLogsTableName"], ConnectionSettings["readingLogsTableSchema"], ConnectionSettings["connectString"]);
            PGSqlUtil.ExecuteNonQuery(new NpgsqlCommand($"INSERT INTO {ConnectionSettings["dbReading_logs"]} (dumpname, createdate, timelastreading) VALUES ('{name}', '{date}', '{dt.AddSeconds(-1)}')"), ConnectionSettings["connectString"]);
        }

        static private string GetTimeLastReading()
        {
            string result = null;
            PGSqlUtil.AutoCreateTable(ConnectionSettings["readingLogsTableName"], ConnectionSettings["readingLogsTableSchema"], ConnectionSettings["connectString"]);
            PGSqlUtil.ExecuteReader(new NpgsqlCommand($"SELECT timelastreading FROM {ConnectionSettings["dbReading_logs"]} ORDER BY timelastreading"), rdr =>
            {
                result = rdr.GetDateTime(0).ToString();
            }, ConnectionSettings["connectString"]);

            if (result == "Npgsql.NpgsqlDataReader")
                    result = "2000-09-22T06:50:32.793+0000";

            return result;
        }
    }

    public class InfobipEvent
    {
        public string definitionId { get; set; }
        public string customeventid { get; set; }
        public string externalPersonId { get; set; }//int
        public string personId { get; set; }//int
        public string occurredTime { get; set; }
        public string eventId { get; set; }
        public Bag Properties { get; set; }
    }
}
