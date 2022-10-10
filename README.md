# SDC-API
## An implentation of the entire backend API for Hack Reactor's System Design Capstone Project

Use ETL scripts to build database:


TODO:

* Create Models
  * ~~Products~~
  * Questions
  * Related
  * ~~Reviews~~

* Create ETL Scripts
  * ~~Products~~
  * Questions
  * Related
  * ~~Reviews~~

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
First, create an array of operations to send to our MongoDB using Mongoose.
Then, open csv as a read stream,
read each line of csv,
parse each line,
create operation to update one document,
add operation to array,
check length of array (this serves as a buffer for bulk operations)
then perform all operations in array if length is reached.

This whole process will add documents to your local MongoDB
based on operations set in functions and csv data given.
Time is tracked once the buffer limit has been reached
to keep track of how long things are taking.

The entire process should take about 20 - 30 minutes based on your
system's hardware.  However, using the basic system requirements of the course,
should complete the whole ETL process in just about 30 - 40 minutes.

Keep track of your system's resources to during the process to
see how much RAM is being used.

Note the console logs in the script.  They are placed only where the buffer limit has been reached.
Removing them will only improve performance in negligible amounts.
It is useful to see them to keep track of progress.