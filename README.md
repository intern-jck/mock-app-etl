# mock-app-etl
## A collection of ETL scripts to build a MongoDB database

### Builds the following collections:

* Products
* Questions
* Answers
* Reviews
* Related
* Cart

### Overview

[ADD LINK TO CSV DATA HERE]

#### Intial

Make sure MongoDB is installed.

Setup:

```bash
npm install
```

#### ETL Script Process
Process is the same for each csv.

* Create an array of operations to send to our MongoDB using Mongoose.
* open csv as a read stream,
* Read each line of csv,
* Parse each line,
* Create operation to update one document,
* Add operation to array of operations to serve as a buffer
* Once buffer limit is reached, bulkWrite operations array
* Repeat until entire CSV file is read

The scripts use promises to build each collection in steps based on the CSV data.
First the main document is created, then updated with relevant data.

Uncomment the collection's promise chain to build the given collection.

If you are feeling confident, the entire database can be built by refactoring the promise chain to include all of the ETL functions.  Be careful though, there are error messages coded in but need updating to get specific error info.  It's best to simply build each collection one at a time.
