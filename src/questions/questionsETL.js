// const fs = require('fs');

const fs = require('node:fs/promises');
const {createReadStream} = require('fs');

const path = require('path');
const csv = require('fast-csv');
const mongoose = require('mongoose');
const Question = require('./QuestionModel.js');
const Answer = require('./AnswerModel.js');

const addQuestions = (csvPath) => {
  let operations = [];
  // Used to measure how long things are taking.
  const t0 = performance.now();

  return new Promise((resolve, reject) => {
    createReadStream(path.resolve(__dirname, csvPath))
    .pipe(csv.parse({ headers: true }))
    .on('error', (error) => (reject(error)))
    .on('data', (row) => {
      // Store everything in an operation
      const operation = {
        updateOne: {
          'filter': {'product_id': row.product_id},
          'update': {
            '$push': {
              'results': {
                'question_id': parseInt(row.id, 10),
                'question_body': row.body,
                'question_date': row.date_written,
                'asker_name': row.asker_name,
                'asker_email': row.asker_email,
                'question_helpfulness': parseInt(row.helpful, 10),
                'reported': row.reported === '1' ? true : false,
              }
            },
          },
          'upsert': true,
        }
      }

      // Add it to the queue.
      operations.push(operation);
      if (operations.length > 100) {
        const tEnd = performance.now();
        console.log(`Bulk Update @ ${Math.round(tEnd - t0)}`);
        Question.bulkWrite(operations);
        operations = [];
      }
    })
    .on('end', (rowCount) => {
      Question.bulkWrite(operations);
      operations = [];
      const tEnd = performance.now();
      console.log(`Added ${rowCount} Questions in ${Math.round(tEnd - t0)}`);
      resolve(rowCount);
    });
  });
};

const addAnswers = (csvPath) => {
  let questionOperations = [];
  let answerOperations = [];
  const t0 = performance.now();
  return new Promise((resolve, reject) => {
    createReadStream(path.resolve(__dirname, csvPath))
    .pipe(csv.parse({ headers: true }))
    .on('error', (error) => (reject(error)))
    .on('data', (row) => {

      // Add the answer id to the question's answer id array
      const questionOperation = {
        updateOne: {
          'filter': {'results.question_id': parseInt(row.question_id, 10)},
          'update': {
            '$push': {
              'results.$.answer_ids': parseInt(row.id, 10)
            },
          },
        }
      };
      // Add it to the queue.
      questionOperations.push(questionOperation);

      const answerOperation = {
        updateOne: {
          'filter': {'answer_id': parseInt(row.id, 10)},
          'update': {
            'body': row.body,
            'date_written': row.date_written,
            'answerer_name': row.asker_name,
            'answerer_email': row.asker_email,
            'helpfulness': parseInt(row.helpful, 10),
            'reported': row.reported === '1' ? true : false,
          },
          'upsert': true,
        }
      }
      // Add it to the queue.
      answerOperations.push(answerOperation);

      if (questionOperations.length > 100) {
        const tEnd = performance.now();
        console.log(`Bulk Update @ ${Math.round(tEnd - t0)}`);
        Question.bulkWrite(questionOperations);
        Answer.bulkWrite(answerOperations);
        answerOperations = [];
        questionOperations = [];
      }
    })
    .on('end', (rowCount) => {
      Question.bulkWrite(questionOperations);
      Answer.bulkWrite(answerOperations);
      answerOperations = [];
      questionOperations = [];
      const tEnd = performance.now();
      console.log(`Added ${rowCount} Answers in ${Math.round(tEnd - t0)}`);
      resolve(rowCount);
    });
  });
};

const addAnswerPhotos = (csvPath) => {
  let operations = [];
  // Used to measure how long things are taking.
  const t0 = performance.now();

  return new Promise((resolve, reject) => {
    createReadStream(path.resolve(__dirname, csvPath))
    .pipe(csv.parse({ headers: true }))
    .on('error', (error) => (reject(error)))
    .on('data', (row) => {
      // Store everything in an operation
      const operation = {
        updateOne: {
          'filter': {'answer_id': parseInt(row.answer_id, 10)},
          'update': {
            '$push': {
              'photos': {
                'id': parseInt(row.id, 10),
                'url': row.url,
                // 'question_date': row.date_written,
                // 'asker_name': row.asker_name,
                // 'asker_email': row.asker_email,
                // 'question_helpfulness': parseInt(row.helpful, 10),
                // 'reported': row.reported === '1' ? true : false,
              }
            },
          },
        }
      }
      // Add it to the queue.
      operations.push(operation);
      if (operations.length > 100) {
        const tEnd = performance.now();
        console.log(`Bulk Update @ ${Math.round(tEnd - t0)}`);
        Answer.bulkWrite(operations);
        operations = [];
      }
    })
    .on('end', (rowCount) => {
      Answer.bulkWrite(operations);
      const tEnd = performance.now();
      console.log(`Added ${rowCount} Answer Photos in ${Math.round(tEnd - t0)}`);
      resolve(rowCount);
    });
  });
};

module.exports = {
  addQuestions,
  addAnswers,
  addAnswerPhotos,
};
