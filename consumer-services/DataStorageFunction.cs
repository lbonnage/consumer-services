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
using System.Web.Http;
using MongoDB.Bson.Serialization.Attributes;
using System.Collections.Generic;
using System.Linq;
using TestInstrumentation;

namespace data_storage
{

    public static class DataStorageFunction
    {

        // Static MongoDB handles for maintaining a persistent connection across service calls
        static MongoClient mongoClient = null;
        static IMongoDatabase mongoConsumerDatabase = null;
        static IMongoCollection<BsonDocument> mongoConfigurationCollection = null;
        static IMongoCollection<AnalysisDocument> mongoAnalysisCollection = null;
        static Dictionary<string, IMongoCollection<BsonDocument>> mongoObjectCollections = new Dictionary<string, IMongoCollection<BsonDocument>>();

        // Static handle to our CustomLogger
        static CustomLogger customLog = null;

        static readonly Dictionary<string, string> aliases = new Dictionary<string, string>()
        {
            { "String", "string" },
            { "Int32", "int" },
            { "Byte", "byte" },
            { "SByte", "sbyte" },
            { "Int16", "short" },
            { "UInt16", "ushort" },
            { "Int64", "long" },
            { "UInt32", "uint" },
            { "UInt64", "ulong" },
            { "Single", "float" },
            { "Double", "double" },
            { "Decimal", "decimal" },
            { "Object", "object" },
            { "Boolean", "bool" },
            { "Char", "char" },
            { "Document", "customobject" }
        };

        [FunctionName("DataStorage")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "post", Route = null)] HttpRequest req,
            ILogger log)
        {

            //log.LogInformation("DataStorage function received a request");

            // Connect to our CustomLogger instance
            if (customLog is null)
            {
                try
                {
                    customLog = new CustomLogger("af437aec-2d91-43d1-9d05-f246fc9a55a0");
                }
                catch (Exception e)
                {
                    //log.LogError("Failed connecting to custom logger: " + e);
                    return new InternalServerErrorResult();
                }
            }

            customLog.RawLog("INFO", "Data storage function received a request");

            // Retrieve the GUID to match data to configuration
            string guid = req.Headers["Config-GUID"];
            if (guid is null)
            {
                //log.LogError("Error retrieving Config-GUID header");
                customLog.RawLog("ERROR", "Error retrieving Config-GUID header");
                return new BadRequestObjectResult("Error retrieving Config-GUID header");
            }
            //log.LogInformation("Retrieved ID: " + guid);
            
            // Parse the message body
            BsonDocument document;
            try
            {
                string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
                object data = JsonConvert.DeserializeObject(requestBody);
                document = BsonDocument.Parse(data.ToString());    // TODO is this the correct way to deserialize and read a BSON object?
            }
            catch (Exception e)
            {
                //log.LogError("Error parsing data: " + e.Message);
                customLog.RawLog("ERROR", "Error parsing data: " + e.Message);
                return new BadRequestObjectResult("Error parsing data: " + e.Message);
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
            if (!mongoObjectCollections.ContainsKey(guid))
            {
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
            //log.LogInformation("Retrieved configuration: " + config.GetValue("field_attributes").ToString());

            // Now you must verify the inputted object data against the configuration
            int badValuesCount, missingFieldsCount, extraFieldsCount;
            (badValuesCount, missingFieldsCount, extraFieldsCount) = RobustnessAnalysis(document, config, log);
            //log.LogInformation("Performed Robustness Analysis: " + badValuesCount + " " + missingFieldsCount + " " + extraFieldsCount);
            customLog.RawLog("INFO", "Performed Robustness Analysis: " + badValuesCount + " " + missingFieldsCount + " " + extraFieldsCount);

            // Update the analysis in the MongoDB database with these new values
            try
            {
                FilterDefinition<AnalysisDocument> filter = Builders<AnalysisDocument>.Filter.Eq("_id", guid);
                var update = Builders<AnalysisDocument>.Update
                    .Inc("NumberOfRecords", 1)
                    .Inc("BadValueCount", badValuesCount > 0 ? 1 : 0)
                    .Inc("MissingFieldCount", missingFieldsCount > 0 ? 1 : 0)
                    .Inc("ExtraFieldCount", extraFieldsCount > 0 ? 1 : 0);
                mongoAnalysisCollection.UpdateOne(filter, update);
            } catch (Exception e)
            {
                //log.LogError("Failed updating analysis document in MongoDB database: " + e.Message);
                customLog.RawLog("ERROR", "Failed updating analysis document in MongoDB database: " + e.Message);
                return new InternalServerErrorResult();
            }

            if (badValuesCount > 0 || missingFieldsCount > 0 || extraFieldsCount > 0)
            {
                //log.LogInformation("Record had some issue, not inserting into database");
                customLog.RawLog("ERROR", "Record had some issue, not inserting into database");
                return new BadRequestObjectResult("Record had some issue, not inserting into database");
            }
            else
            {
                // Insert the received object into the MongoDB object collection
                //log.LogInformation("Inserting document into MongoDB");
                try
                {
                    mongoObjectCollections[guid].InsertOne(document);
                }
                catch (Exception e)
                {
                    //log.LogError("Failed inserting document into MongoDB database: " + e.Message);
                    customLog.RawLog("ERROR", "Failed inserting document into MongoDB database: " + e.Message);
                    return new InternalServerErrorResult();
                }

                // Increment the record count to reflect this insertion
                var update = Builders<AnalysisDocument>.Update
                    .Inc("NumberOfRecords", 1);
            }

            customLog.RawLog("INFO", "Succeeded in inserting document with ID: " + guid);
            return new OkObjectResult("Succeeded in inserting document");

        }

        /**
         * 
         * Using the supplied configuration, checks the document for the following:
         * - bad values
         * - missing fields
         * - extra fields
         */
        public static (int, int, int) RobustnessAnalysis(BsonDocument document, BsonDocument config, ILogger log)
        {

            int badValuesCount = 0, missingFieldsCount = 0, extraFieldsCount = 0;

            // Create a dictionary mapping the field name to its corresponding document
            Dictionary<string, BsonDocument> expectedTypes = new Dictionary<string, BsonDocument>();
            config.GetValue("field_attributes").AsBsonArray.ToList().ForEach(val => expectedTypes[val.AsBsonDocument["name"].AsString] = val.AsBsonDocument);
            
            // Create lists of the string names of the expected fields and the string names of the actual fields
            List<string> expectedFields = config.GetValue("field_attributes").AsBsonArray.ToList().ConvertAll<string>(val => val.AsBsonDocument.GetElement("name").Value.AsString);
            List<string> actualFields = document.Elements.ToList().ConvertAll<string>(val => val.Name);

            // Create a list of extra fields to remove from the document once iteration is completed
            List<string> extraFields = new List<string>();

            foreach (BsonElement element in document)
            {

                string name = element.Name;
                BsonValue value = element.Value;

                BsonDocument expectedDocument;

                // Check if the field exists in the config
                if (expectedTypes.TryGetValue(name, out expectedDocument))
                {

                    string expectedType = expectedDocument["type"].AsString;

                    // Check if the field has the correct type
                    string fieldType = aliases[value.BsonType.ToString()];
                    if (expectedType == fieldType)
                    {

                        if (fieldType == "customobject")
                        {
                            // Call RobustnessAnalysis recursively on the nested object
                            int nestedBadValuesCount, nestedMissingFieldsCount, nestedExtraFieldsCount;
                            BsonDocument nestedDocument = value.AsBsonDocument;
                            (nestedBadValuesCount, nestedMissingFieldsCount, nestedExtraFieldsCount) = RobustnessAnalysis(nestedDocument, expectedDocument, log);

                            badValuesCount += nestedBadValuesCount;
                            missingFieldsCount += nestedMissingFieldsCount;
                            extraFieldsCount += nestedExtraFieldsCount;

                        }

                    } else
                    {
                        badValuesCount++;
                        //log.LogError("Incorrect value type.  Received " + fieldType + " when we expected " + expectedType + ".");
                    }


                } else
                {
                    extraFields.Add(name);
                    extraFieldsCount++;
                    //log.LogError("Extra field detected.  Received field " + name + ".");
                }

            }

            // Check for missing fields
            foreach (string expectedField in expectedFields)
            {
                if (!actualFields.Contains(expectedField))
                {
                    missingFieldsCount++;
                    //log.LogError("Missing field detected.  Missing field " + expectedField + ".");
                }
            }

            return (badValuesCount, missingFieldsCount, extraFieldsCount);


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

    //public class Configuration
    //{
    //    [BsonId]
    //    public string Id { get; set; }

    //    public BsonArray field_attributes { get; set; }
    //}

}
