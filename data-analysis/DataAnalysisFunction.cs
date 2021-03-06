using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using MongoDB.Driver;
using MongoDB.Bson;
using System.Collections.Generic;
using MongoDB.Bson.Serialization.Attributes;
using System.Web.Http;
using TestInstrumentation;

namespace data_analysis
{

    public static class DataAnalysisFunction
    {

        // Static MongoDB handles for maintaining a persistent connection across service calls
        static MongoClient mongoClient = null;
        static IMongoDatabase mongoConsumerDatabase = null;
        static IMongoCollection<BsonDocument> mongoConfigurationCollection = null;
        static IMongoCollection<AnalysisDocument> mongoAnalysisCollection = null;
        static Dictionary<string, IMongoCollection<BsonDocument>> mongoObjectCollections = new Dictionary<string, IMongoCollection<BsonDocument>>();

        // Static handle to our CustomLogger
        static CustomLogger customLog = null;

        // Static list of numerical data types for performing numerical analysis on
        static readonly List<string> numericalTypes = new List<string> { "int", "short", "ushort", "long", "uint", "ulong", "float", "double", "decimal" };


        [FunctionName("DataAnalysis")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", Route = null)] HttpRequest req,
            ILogger log)
        {

            ////log.LogInformation("Data analysis function received a request");

            // Connect to our CustomLogger instance
            if (customLog is null)
            {
                try
                {
                    customLog = new CustomLogger("362043e7-c575-4a03-9c75-edda0be22351");
                }
                catch (Exception e)
                {
                    //log.LogError("Failed connecting to custom logger: " + e);
                    return new InternalServerErrorResult();
                }
            }

            customLog.RawLog("INFO", "Data analysis function received a request");

            // Retrieve the GUID generated for this specific configuration
            string guid = req.Headers["Config-GUID"];
            if (guid is null)
            {
                //log.LogError("Error retrieving Config-GUID header");
                customLog.RawLog("ERROR", "Error retrieving Config-GUID header");
                return new BadRequestObjectResult("Error retrieving Config-GUID header");
            }

            // In order to preserve the MongoDB client connection across Service calls, perform this check and only connect if necessary
            if (mongoClient is null)
            {
                //log.LogInformation("Connecting to MongoDB Database...");
                try
                {
                    mongoClient = new MongoClient(System.Environment.GetEnvironmentVariable("MongoDBAtlasConnectionString"));
                    mongoConsumerDatabase = mongoClient.GetDatabase(System.Environment.GetEnvironmentVariable("DeploymentEnvironment"));
                    mongoConfigurationCollection = mongoConsumerDatabase.GetCollection<BsonDocument>("configurations");
                    mongoAnalysisCollection = mongoConsumerDatabase.GetCollection<AnalysisDocument>("analysis");
                    //log.LogInformation("Connected to MongoDB Database");
                }
                catch (Exception e)
                {
                    //log.LogError("Error connecting to MongoDB database: " + e.Message);
                    customLog.RawLog("FATAL", "Error connecting to MongoDB database: " + e.Message);
                    return new InternalServerErrorResult();
                }
            }

            // Make sure you have a handle to the collection for this kind of object
            if (!mongoObjectCollections.ContainsKey(guid)) {
                try
                {
                    mongoObjectCollections[guid] = mongoConsumerDatabase.GetCollection<BsonDocument>(guid);
                }
                catch (Exception e)
                {
                    //log.LogError("Error retrieving object collection from MongoDB database: " + e.Message);
                    customLog.RawLog("ERROR", "Error retrieving object collection from MongoDB database: " + e.Message);
                    return new InternalServerErrorResult();
                }
            }


            // Retrieve the correct configuration document from the MongoDB database
            //log.LogInformation("Attempting to retrieve configuration for ID: " + guid);
            BsonDocument config;
            try
            {
                FilterDefinition<BsonDocument> filter = Builders<BsonDocument>.Filter.Eq("_id", guid);
                config = mongoConfigurationCollection.Find<BsonDocument>(filter).First<BsonDocument>();
            }
            catch (Exception e)
            {
                //log.LogError("Error retrieving configuration for data: " + e.Message);
                customLog.RawLog("ERROR", "Error retrieving configuration for data: " + e.Message);
                return new BadRequestObjectResult("Error retrieving configuration for data: " + e.Message);
            }

            // Retrieve the current analysis for this ID from the database
            //log.LogInformation("Attempting to retrieve analysis for ID: " + guid);
            AnalysisDocument analysis;
            try
            {
                FilterDefinition<AnalysisDocument> filter = Builders<AnalysisDocument>.Filter.Eq("_id", guid);
                analysis = mongoAnalysisCollection.Find<AnalysisDocument>(filter).First<AnalysisDocument>();
            }
            catch (Exception e)
            {
                //log.LogError("Error retrieving analysis for data: " + e.Message);
                customLog.RawLog("ERROR", "Error retrieving analysis for data: " + e.Message);
                return new BadRequestObjectResult("Error retrieving configuration for data: " + e.Message);
            }

            // Perform statistical analysis on the documents in the desired collection
            BsonArray statisticalAnalysis = analysis.StatisticalAnalysis;
            StatisticalAnalysis(ref statisticalAnalysis, mongoObjectCollections[guid], "", log);

            analysis.StatisticalAnalysis = statisticalAnalysis;

            // Return the completed analysis to the requestor
            customLog.RawLog("INFO", "Successfully generated and returned analysis for ID: "+ guid);
            return new OkObjectResult(analysis.ToJson());
        }

        /**
         * Performs statistical analysis on all the documents for a specific set of objects
         * Should be able to find the following for all appropriate elements:
         *  - Mean (numerical values)
         *  - Standard deviation (numerical values)
         */
         public static void StatisticalAnalysis(ref BsonArray statisticalAnalysis, IMongoCollection<BsonDocument> collection, string path, ILogger log)
        {

            foreach (BsonDocument fieldStatistic in statisticalAnalysis)
            {
                
                string name = fieldStatistic.GetValue("name").ToString();
                string type = fieldStatistic.GetValue("type").ToString();

                if (numericalTypes.Contains(type))
                {

                    string fieldPath = "$" + path + (path.Length == 0 ? "" : ".") + name;

                    var pipeline = new BsonDocument[]
                    {
                        new BsonDocument { { "$group", new BsonDocument { { "_id", BsonNull.Value }, { "mean", new BsonDocument { {"$avg", fieldPath  } } }, { "standard deviation", new BsonDocument { { "$stdDevPop", fieldPath } } } } } }
                    };

                    BsonDocument result = collection.Aggregate<BsonDocument>(pipeline).First<BsonDocument>();

                    fieldStatistic.Set("mean", result.GetValue("mean"));
                    fieldStatistic.Set("standard deviation", result.GetValue("standard deviation"));

                } else if (type == "customobject")
                {
                    BsonArray nestedStatisticalAnalysis = fieldStatistic.GetValue("StatisticalAnalysis").AsBsonArray;
                    StatisticalAnalysis(ref nestedStatisticalAnalysis, collection, (path.Length == 0 ? "" : path + ".") + name, log);
                }

            }

        }

    }

    /**
      * Class used to represent an Analysis of each record type
      * There should be one instance of this in the Analysis database for each config
      */
    public class AnalysisDocument
    {

        [BsonId]
        public string ID { get; set; }

        // the number of records for this ID
        [BsonElement("NumberOfRecords")]
        public int NumberOfRecords { get; set; }

        // invalid values for the type of field, including null
        [BsonElement("BadValueCount")]
        public int BadValueCount { get; set; }

        // data records with missing fields
        [BsonElement("MissingFieldCount")]
        public int MissingFieldCount { get; set; }

        // data records with unexpected fields
        [BsonElement("ExtraFieldCount")]
        public int ExtraFieldCount { get; set; }

        // keep track of one-time total failures
        [BsonElement("OneTimeCommunicationFailureCount")]
        public int OneTimeCommunicationFailureCount { get; set; }

        // keep track of one-time total failures
        [BsonElement("IntermittentCommunicationFailureCount")]
        public int IntermittentCommunicationFailureCount { get; set; }

        // array containing statistical analysis of all relevant data within the document
        public BsonArray StatisticalAnalysis { get; set; }

        public AnalysisDocument(string id)
        {
            this.ID = id;
            this.BadValueCount = 0;
            this.MissingFieldCount = 0;
            this.ExtraFieldCount = 0;
            this.OneTimeCommunicationFailureCount = 0;
            this.IntermittentCommunicationFailureCount = 0;
        }

    }


}
