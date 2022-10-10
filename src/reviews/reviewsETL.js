const fs = require('fs');
const path = require('path');
const csv = require('fast-csv');
const mongoose = require('mongoose');

// Found using  cvsHelpers.js in helpers folder.
// Used to show progress when adding to database.
const reviewsLength = 5774952;
const photosLength = 2742540;
const chracteristicsLength = 3347679;
const reviewChracteristicsLength = 19327575;

const Review = require('./ReviewModel.js');

// Process is the same for each csv.
// First, create an array of operations to send to our MongoDB using Mongoose.
// Then, open csv as a read stream,
// read each line of csv,
// parse each line,
// create operation to update one document,
// add operation to array,
// check length of array (this serves as a buffer for bulk operations)
// then perform all operations in array if length is reached.

// This whole process will add documents to your local MongoDB
// based on operations set in functions and csv data given.
// Time is tracked once the buffer limit has been reached
// to keep track of how long things are taking.

// The entire process should take about 20 - 30 minutes based on your
// system's hardware.  However, using the basic system requirements of the course,
// should complete the whole ETL process in just about 30 - 40 minutes.

// Keep track of your system's resources to during the process to
// see how much RAM is being used.

// Note the console logs in the script.  They are placed only where the buffer limit has been reached.
// Removing them will only improve performance in negligible amounts.
// It is useful to see them to keep track of progress.

const addReviews = (csvPath) => {

  let operations = [];

  // Used to measure how long things are taking.
  const t0 = performance.now();

  fs.createReadStream(path.resolve(__dirname, csvPath))
  .pipe(csv.parse({ headers: true }))
  .on('error', error => console.error(error))
  .on('data', (row) => {
    // Need to store multiple inc operations in and object.
    const incUpdates = {};
    // Increment meta.ratings by 1
    incUpdates['meta.ratings.' + row.rating] = 1;
    // Increment meta.recommended by 1
    if (row.recommend === 'false') {
      incUpdates['meta.recommended.0'] = 1;
    } else if (row.recommend === 'true') {
      incUpdates['meta.recommended.1'] = 1;
    }
    // Store everything in an operation
    const reviewOP = {
      updateOne: {
        'filter': { 'product_id': row.product_id},
        'update': {
          '$push': {
            'results': {
              'id':  row.id,
              'rating':  row.rating,
              'date': row.date,
              'summary': row.summary,
              'body': row.body,
              'recommend': row.recommend,
              'reported': row.reported,
              'reviewer_name': row.reviewer_name,
              'reviewer_email': row.reviewer_email,
              'response': row.response,
              'helpfulness': row.helpfulness,
            }
          },
          '$inc': incUpdates
        },
        'upsert': true,
      }
    }
    // Add it to the queue.
    operations.push(reviewOP);
    if (operations.length > 10000) {
      const tEnd = performance.now();
      console.log(`Bulk Update @ ${Math.round(tEnd - t0)} : ${Math.round((parseInt(row.id)/ reviewsLength) * 100)}%`);
      Review.bulkWrite(operations);
      operations = [];
    }
  })
  .on('end', (rowCount) => {

    Review.bulkWrite(operations);
    const tEnd = performance.now();
    console.log(`Added ${rowCount} rows in ${Math.round(tEnd - t0)}`);

    // Hacky way to keep track of reviews
    const filter = { 'product_id': 0};
    const update = { '$set': { 'review_count': rowCount } }
    const options = {
      new: true,
      strict: false,
      upsert: true
    }
    Review.findOneAndUpdate(filter, update, options).then((doc) => ( console.log(doc)));
  });
};

const addPhotos = (csvPath) => {

  let operations = [];
  const t0 = performance.now();

  fs.createReadStream(path.resolve(__dirname, csvPath))
  .pipe(csv.parse({ headers: true }))
  .on('error', error => console.error(error))
  .on('data', (row) => {

    const photosOP = {
      updateOne: {
        'filter': { "results.id": row.review_id },
        'update': {
          '$push': { 'results.$.photos': {
            'id': row.id,
            'url': row.url,
          }},
          'upsert': true
        },
      }
    };

    operations.push(photosOP);

    if (operations.length > 2500) {
      Review.bulkWrite(operations);
      const tEnd = performance.now();
      console.log(`Bulk Update @ ${Math.round(tEnd - t0)} : ${Math.round((parseInt(row.id)/ photosLength) * 100)}%`);
      operations = [];
    }

  })
  .on('end', (rowCount) => {
    Review.bulkWrite(operations);
    const tEnd = performance.now();
    console.log(`Added ${rowCount} rows in ${Math.round(tEnd - t0)}`)
  });
};

const addCharacteristics = (csvPath) => {

  let operations = [];
  const t0 = performance.now();

  fs.createReadStream(path.resolve(__dirname, csvPath))
  .pipe(csv.parse({ headers: true }))
  .on('error', error => console.error(error))
  .on('data', (row) => {

    const newCharacteristic = {};
    newCharacteristic['meta.characteristics.' + row.id] = {
      name: row.name,
      value: []
    };

    const updateOne = {
      updateOne: {
        'filter': { 'product_id': row.product_id },
        'update': { '$set': newCharacteristic },
      }
    };

    operations.push(updateOne)

    if(operations.length > 2500) {
      Review.bulkWrite(operations);
      const tEnd = performance.now();
      console.log(`Bulk Update @ ${Math.round(tEnd - t0)} : ${Math.round((parseInt(row.id)/ chracteristicsLength) * 100)}%`);
      operations = [];
    }

  })
  .on('end', (rowCount) => {
    Review.bulkWrite(operations);
    const tEnd = performance.now();
    console.log(`Added ${rowCount} rows in ${Math.round(tEnd - t0)}`);
  });

};

const updateCharacteristics = (csvPath) => {

  let operations = [];
  const t0 = performance.now();

  fs.createReadStream(path.resolve(__dirname, csvPath))
  .pipe(csv.parse({ headers: true }))
  .on('error', error => console.error(error))
  .on('data', (row) => {

    const updateCharacteristic = {};
    updateCharacteristic['meta.characteristics.' + row.characteristic_id + '.value'] = parseInt(row.value);

    const updateOne = {
      updateOne: {
        'filter': { 'results.id': row.review_id },
        'update': { '$push': updateCharacteristic },
      }
    };

    operations.push(updateOne)

    if(operations.length > 2500) {
      Review.bulkWrite(operations);
      const tEnd = performance.now();
      console.log(`Bulk Update @ ${Math.round(tEnd - t0)} : ${Math.round((parseInt(row.id)/ reviewChracteristicsLength) * 100)}%`);
      operations = [];
    }

  })
  .on('end', (rowCount) => {
    Review.bulkWrite(operations);
    const tEnd = performance.now();
    console.log(`Added ${rowCount} rows in ${Math.round(tEnd - t0)}`)
  });

};


module.exports = {
  addReviews,
  addPhotos,
  addCharacteristics,
  updateCharacteristics
};