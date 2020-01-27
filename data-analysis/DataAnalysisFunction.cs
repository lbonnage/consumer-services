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

        // Static list of numerical data types for performing numerical analysis on
        static readonly List<string> numericalTypes = new List<string> { "int", "short", "ushort", "long", "uint", "ulong", "float", "double", "decimal" };


        [FunctionName("DataAnalysis")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", Route = null)] HttpRequest req,
            ILogger log)
        {
            log.LogInformation("Data analysis function received a request");

            // Retrieve the GUID generated for this specific configuration
            string guid = req.Headers["Config-GUID"];
            if (guid is null)
            {
                log.LogError("Error retrieving Config-GUID header");
                return new BadRequestObjectResult("Error retrieving Config-GUID header");
            }

            // In order to preserve the MongoDB client connection across Service calls, perform this check and only connect if necessary
            if (mongoClient is null)
            {
                log.LogInformation("Connecting to MongoDB Database...");
                try
                {
                    mongoClient = new MongoClient(System.Environment.GetEnvironmentVariable("MongoDBAtlasConnectionString"));
                    mongoConsumerDatabase = mongoClient.GetDatabase(System.Environment.GetEnvironmentVariable("DeploymentEnvironment"));
                    mongoConfigurationCollection = mongoConsumerDatabase.GetCollection<BsonDocument>("configurations");
                    mongoAnalysisCollection = mongoConsumerDatabase.GetCollection<AnalysisDocument>("analysis");
                    log.LogInformation("Connected to MongoDB Database");
                }
                catch (Exception e)
                {
                    log.LogError("Error connecting to MongoDB database: " + e.Message);
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
                    log.LogError("Error retrieving object collection from MongoDB database: " + e.Message);
                    return new InternalServerErrorResult();
                }
            }


            // Retrieve the correct configuration document from the MongoDB database
            log.LogInformation("Attempting to retrieve configuration for ID: " + guid);
            BsonDocument config;
            try
            {
                FilterDefinition<BsonDocument> filter = Builders<BsonDocument>.Filter.Eq("_id", guid);
                config = mongoConfigurationCollection.Find<BsonDocument>(filter).First<BsonDocument>();
            }
            catch (Exception e)
            {
                log.LogError("Error retrieving configuration for data: " + e.Message);
                return new BadRequestObjectResult("Error retrieving configuration for data: " + e.Message);
            }

            // Retrieve the current analysis for this ID from the database
            log.LogInformation("Attempting to retrieve analysis for ID: " + guid);
            AnalysisDocument analysis;
            try
            {
                FilterDefinition<AnalysisDocument> filter = Builders<AnalysisDocument>.Filter.Eq("_id", guid);
                analysis = mongoAnalysisCollection.Find<AnalysisDocument>(filter).First<AnalysisDocument>();
            }
            catch (Exception e)
            {
                log.LogError("Error retrieving analysis for data: " + e.Message);
                return new BadRequestObjectResult("Error retrieving configuration for data: " + e.Message);
            }

            // Retrieve a cursor to the objects in the appropriate collection
            log.LogInformation("Attempting to retrieve list for collection: " + guid);
            List<BsonDocument> documents;
            try
            {
                documents = mongoObjectCollections[guid].Find<BsonDocument>(f => true).ToCursor().ToList();
            } catch (Exception e)
            {
                log.LogError("Error retrieving collection for data: " + e.Message);
                return new BadRequestObjectResult("Error retrieving collection for data: " + e.Message);
            }

            BsonArray statisticalAnalysis = analysis.StatisticalAnalysis;

            // Iterate over all the documents in the collection and perform the analysis step on them
            foreach (BsonDocument document in documents)
            {
                StatisticalAnalysis(ref statisticalAnalysis, config, document, log);
            }

            // Return the completed analysis to the requestor
            return new OkObjectResult(analysis.ToJson());
        }

        /**
         * Performs statistical analysis on all the documents for a specific set of objects
         * Should be able to find the following for all appropriate elements:
         *  - Mean (numerical values)
         *  - Standard deviation (numerical values)
         */
        public static void StatisticalAnalysis(ref BsonArray statisticalAnalysis, BsonDocument config, BsonDocument document, ILogger log)
        {
            
            foreach (BsonDocument fieldStatistic in statisticalAnalysis)
            {
                string name = fieldStatistic.GetValue("name").ToString();
                string type = fieldStatistic.GetValue("type").ToString();

                if (document.ContainsValue(name))
                {
                    BsonValue value = document.GetValue(name);
                    BsonArray values = document.GetValue("values").AsBsonArray;
                    fieldStatistic.Set("count", fieldStatistic.GetValue("count").AsInt32 + 1);

                    // Calculate mean and standard deviation with numerical 
                    if (numericalTypes.Contains(type))
                    {
                        switch (type)
                        {
                            case "int":
                                int intValue = value.AsInt32;
                                break;
                            case "short":
                                break;
                            case "ushort":
                                break;
                            case "long":
                                long longValue = value.AsInt64;
                                break;
                            case "uint":
                                break;
                            case "ulong":
                                break;
                            case "float":
                                break;
                            case "double":
                                double doubleValue = value.AsDouble;
                                break;
                            case "decimal":
                                decimal decimalValue = value.AsDecimal;
                                break;
                        }
                    } else if (type == "customobject")
                    {

                    }

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
