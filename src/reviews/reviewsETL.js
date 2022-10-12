const fs = require('node:fs/promises');
const {createReadStream} = require('fs');
const path = require('path');
const csv = require('fast-csv');
const mongoose = require('mongoose');
const Review = require('./ReviewModel.js');

const addReviews = (csvPath) => {
  let operations = [];
  // Used to measure how long things are taking.
  const t0 = performance.now();

  return new Promise((resolve, reject) => {
    createReadStream(path.resolve(__dirname, csvPath))
    .pipe(csv.parse({ headers: true }))
    .on('error', (error) => (reject(error)))
    .on('data', (row) => {
      // Need to store multiple inc operations in and object.
      const thingsToIncrement = {};
      // Increment meta.ratings by 1
      thingsToIncrement['meta.ratings.' + row.rating] = 1;
      // Increment meta.recommended by 1
      if (row.recommend === 'false') {
        thingsToIncrement['meta.recommended.0'] = 1;
      } else if (row.recommend === 'true') {
        thingsToIncrement['meta.recommended.1'] = 1;
      }
      // Store everything in an operation
      const operation = {
        updateOne: {
          'filter': { 'product_id': parseInt(row.product_id, 10)},
          'update': {
            '$push': {
              'results': {
                'review_id':  parseInt(row.id),
                'rating':  row.rating,
                'date': row.date,
                'summary': row.summary,
                'body': row.body,
                'reviewer_name': row.reviewer_name,
                'reviewer_email': row.reviewer_email,
                'response': row.response,
                'helpfulness': row.helpfulness,
                'recommend': row.recommend,
                'reported': row.reported,
              }
            },
            '$inc': thingsToIncrement
          },
          'upsert': true,
        }
      }
      // Add it to the queue.
      operations.push(operation);
      if (operations.length > 2500) {
        const tEnd = performance.now();
        console.log(`Bulk Update @ ${Math.round(tEnd - t0)}`);
        Review.bulkWrite(operations);
        operations = [];
      }
    })
    .on('end', (rowCount) => {
      Review.bulkWrite(operations);
      const tEnd = performance.now();
      console.log(`Added ${rowCount} rows in ${Math.round(tEnd - t0)}`);
      resolve(rowCount);
    });
  });
};

const addReviewPhotos = (csvPath) => {

  let operations = [];
  const t0 = performance.now();
  return new Promise((resolve, reject) => {
    createReadStream(path.resolve(__dirname, csvPath))
    .pipe(csv.parse({ headers: true }))
    .on('error', (error) => (reject(error)))
    .on('data', (row) => {

      const operation = {
        updateOne: {
          'filter': { 'results.review_id': row.review_id },
          'update': {
            '$push': { 'results.$.photos': {
              'id': parseInt(row.id, 10),
              'url': row.url,
            }}
          },
        }
      };
      operations.push(operation);
      if (operations.length > 2500) {
        Review.bulkWrite(operations);
        const tEnd = performance.now();
        console.log(`Bulk Update @ ${Math.round(tEnd - t0)}`);
        operations = [];
      }

    })
    .on('end', (rowCount) => {
      Review.bulkWrite(operations);
      const tEnd = performance.now();
      console.log(`Added ${rowCount} rows in ${Math.round(tEnd - t0)}`)
      resolve(rowCount);
    });
  });
};

const addReviewCharacteristics = (csvPath) => {

  let operations = [];
  const t0 = performance.now();
  return new Promise((resolve, reject) => {
    createReadStream(path.resolve(__dirname, csvPath))
    .pipe(csv.parse({ headers: true }))
    .on('error', (error) => (reject(error)))
    .on('data', (row) => {
      const newCharacteristic = {};
      newCharacteristic[row.id] = {
        // 'id': parseInt(row.id, 10),
        'name': row.name,
        'value': []
      };
      const operation = {
        updateOne: {
          'filter': { 'product_id': parseInt(row.product_id, 10) },
          'update': {
            '$set': {
              'meta.characteristics': newCharacteristic
            },
          },
        }
      };
      operations.push(operation);
      if(operations.length > 2500) {
        Review.bulkWrite(operations);
        const tEnd = performance.now();
        console.log(`Bulk Update @ ${Math.round(tEnd - t0)}`);
        operations = [];
      }
    })
    .on('end', (rowCount) => {
      Review.bulkWrite(operations);
      const tEnd = performance.now();
      console.log(`Added ${rowCount} rows in ${Math.round(tEnd - t0)}`);
        resolve(rowCount);
    });
  });

};

const updateReviewCharacteristics = (csvPath) => {

  let operations = [];
  const t0 = performance.now();
  return new Promise((resolve, reject) => {
    createReadStream(path.resolve(__dirname, csvPath))
    .pipe(csv.parse({ headers: true }))
    .on('error', (error) => (reject(error)))
    .on('data', (row) => {

      const updateCharacteristic = {};
      updateCharacteristic['meta.characteristics.' + row.characteristic_id + '.value'] = parseInt(row.value);

      const operation = {
        updateOne: {
          'filter': {'results.review_id': row.review_id},
          'update': {
            '$push': updateCharacteristic,
          },
        }
      };

      operations.push(operation)
      if(operations.length > 2500) {
        Review.bulkWrite(operations);
        const tEnd = performance.now();
        console.log(`Bulk Update @ ${Math.round(tEnd - t0)}`);
        operations = [];
      }
    })
    .on('end', (rowCount) => {
      Review.bulkWrite(operations);
      const tEnd = performance.now();
      console.log(`Added ${rowCount} rows in ${Math.round(tEnd - t0)}`)
      resolve(rowCount);
    });
  });
};

module.exports = {
  addReviews,
  addReviewPhotos,
  addReviewCharacteristics,
  updateReviewCharacteristics
};
