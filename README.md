
# Warmup Project Consumer

Navigation: [Project Wiki](https://laser.cs.rice.edu/classes/comp410/s20/_layouts/15/start.aspx#/SitePages/Warm-up%20Project%20Team%20A.aspx) | [Producer Repository](https://example.com) | **Consumer Repository**

## Description
This is the source code for Team A's Warmup Project Consumer.  This consumer is capable of accepting and analyzing highly-customizable data records sent via HTTP.
Our consumer is split into three microservices, each built using an Azure Function.

**Configuration Service**: This service is used to configure the consumer to accept your desired form of data record.  Without a valid configuration, a data record cannot be stored or analyzed.

**Data Storage Service**: This service is used to accept and store data records sent to the consumer.

**Data Analysis Service**: This service is used to retrieve analysis on data records previously accepted by the consumer.  This analysis is performed on all of the data records of a single type, with types being determined by the configurations sent to the Configuration Service.
Here is an image depicting the various services and their connectivity:

![Services](https://i.imgur.com/Tm1kbZ0.png)

## Instructions

### Interact with API

Following is a step-by-step example of interacting with the consumer API.  For this example, we will attempting to send unique *classroom* objects to the consumer, and then receive analysis of the various fields present.

For reference, an example *classroom* object:
```json
{
  "classroomName": "Duncan Hall 1072",
  "classroomLimit": 65,
  "professor": 
    {
      "name": "Swong",
      "subject": "Computer Science",
      "yearsAtRice": 10
    },
  "student":
    {
      "name": "Liam",
      "subject": "Computer Science"
    },
  "class":
    {
      "name": "COMP 410",
      "averageGrade": 75.6
    }
}
```

**Important Note**: For all requests to the consumer, you must include a custom *Config-GUID* header in the HTTP request.  This header should be the name of the type of record you are trying to configure, send, or analyze (in this example it would be *classroom*).  This header must be present on all requests, GET or POST.

Example:
1. In order to configure the consumer to receieve and analyze this *classroom* object, we must send the appropriate configuration to the **Configuration Service**.  You will send this configuration via a POST request to the *Configuration Service* endpoint.  A configuration for this object would look like this:
```json
{
  "field_attributes": [
    {
      "name": "classroomName",
      "type": "string"
    },
    {
      "name": "classroomLimit",
      "type": "int"
    },
    {
      "name": "professor",
      "type": "customobject",
      "field_attributes": [
        {
          "name": "name",
          "type": "string"
        },
        {
          "name": "subject",
          "type": "string"
        },
        {
          "name": "yearsAtRice",
          "type": "int"
        }
      ]
    },
    {
      "name": "student",
      "type": "customobject",
      "field_attributes": [
        {
          "name": "name",
          "type": "string"
        },
        {
          "name": "subject",
          "type": "string"
        }
      ]
    },
    {
      "name": "class",
      "type": "customobject",
      "field_attributes": [
        {
          "name": "name",
          "type": "string"
        },
        {
          "name": "averageGrade",
          "type": "double"
        }
      ]
    },
  ]
}
```

2. You may now send as many instances of the *classroom* object to the *Data Storage Service* endpoint via HTTP POST as desired.  You do this by sending the object as a JSON.
3. You may send a GET request to the *Data Analysis* service in order to receive analytics on all of the *classroom* objects that have been sent to the consumer so far.

### Debug locally

To run the project locally, you first need to clone the repository:
```
git clone https://github.com/lbonnage/consumer-team-a.git
```
Once cloned, open the directory in Visual Studio Code 2019.  In the *Solution Explorer* window, you should see three function directories, *configuration*, *data-analysis*, and *data-storage*.  These are the directories for each of the individual services.

In order to run these three functions you will need the connection strings for the MongoDB database located in a *local.settings.json* file in each of the three directories.  This means you must place the same file in the top-level of each of these three directories.  The file is available on Slack.

In the program start dropdown (default location is the top of the window) you should see three options, one for each of the aforementioned services.  To run one, you must select it in the dropdown and navigate to '*Debug -> Start Without Debugging*'.  This will open a console for the function.  Once the function is set up it will provide the endpoint to which you can send requests.

You may then follow the above instructions to debug the services.  Any error output will be viewable on the respective service's console.
