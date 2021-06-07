## SQS (with camel-based consumers)

The application starts from the main class

The main class declares a bean **camel-context** for camel which

- adds **sqsClient** (an instance of AmazonSQSClient) in camel's registry
- enrolls **SqsCamelRouteBuilder** as camel's route

Then  Camel internally invokes **SqsCamelRouteBuilder.configure()** and below things start happening 
* The **SqsQueueNameManager** bean is used to fetch the list of queue names
* for each queue names
    * populates route uri
    * defines custom predicate object
    * defines route depending upon the custom predicate  
