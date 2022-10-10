const fs = require('fs');
const path = require('path');
const csv = require('fast-csv');
const mongoose = require('mongoose');
const Question = require('./QuestionModel.js');

const addQuestions = (csvPath) => {
  let operations = [];
  // Used to measure how long things are taking.
  const t0 = performance.now();

  fs.createReadStream(path.resolve(__dirname, csvPath))
  .pipe(csv.parse({ headers: true }))
  .on('error', error => console.error(error))
  .on('data', (row) => {
    // Store everything in an operation
    const productOperation = {
      updateOne: {
        'filter': {'product_id': row.product_id},
        'update': {
          '$push': {
            'results': {
              'question_id':  row.id,
              'question_body':  row.body,
              'question_date': row.date_written,
              'asker_name': row.asker_name,
              'asker_email': row.asker_email,
              'question_helpfulness': row.helpful,
              'reported': row.reported,
            }
          },
        },
        'upsert': true,
      }
    }

    // Add it to the queue.
    operations.push(productOperation);
    if (operations.length > 100) {
      const tEnd = performance.now();
      console.log(`Bulk Update @ ${Math.round(tEnd - t0)}`);
      Question.bulkWrite(operations);
      operations = [];
    }
  })
  .on('end', (rowCount) => {
    Question.bulkWrite(operations);
    const tEnd = performance.now();
    console.log(`Added ${rowCount} rows in ${Math.round(tEnd - t0)}`);
  });
};

const addAnswers = (csvPath) => {
  let operations = [];
  // Used to measure how long things are taking.
  const t0 = performance.now();

  fs.createReadStream(path.resolve(__dirname, csvPath))
  .pipe(csv.parse({ headers: true }))
  .on('error', error => console.error(error))
  .on('data', (row) => {
    // Store everything in an operation
    const productOperation = {
      updateOne: {
        'filter': {'results.question_id': row.question_id},
        'update': {
          '$push': {
            'results.$.answer_ids': row.id
          },
        },
        // 'upsert': true,
      }
    }

    // Add it to the queue.
    operations.push(productOperation);
    if (operations.length > 100) {
      const tEnd = performance.now();
      console.log(`Bulk Update @ ${Math.round(tEnd - t0)}`);
      Question.bulkWrite(operations);
      operations = [];
    }
  })
  .on('end', (rowCount) => {
    Question.bulkWrite(operations);
    const tEnd = performance.now();
    console.log(`Added ${rowCount} rows in ${Math.round(tEnd - t0)}`);
  });
};

module.exports = {
  addQuestions,
  addAnswers,
  // addAnswerPhotos
};
