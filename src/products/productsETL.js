// const fs = require('fs');
const fs = require('node:fs/promises');
const {createReadStream} = require('fs');

const path = require('path');
const csv = require('fast-csv');
const mongoose = require('mongoose');
const Product = require('./ProductModel.js');

const addProducts = (csvPath) => {
// async function addProducts(csvPath) {
  let operations = [];
  // Used to measure how long things are taking.
  const t0 = performance.now();

  return new Promise((resolve, reject) => {

    createReadStream(path.resolve(__dirname, csvPath))
    .pipe(csv.parse({ headers: true }))
    .on('error', (error) => (reject(error)))
    .on('data', (row) => {
      // Store everything in an operation
      const productId = parseInt(row.id, 10);
      const productOperation = {
        updateOne: {
          'filter': {'product_id': productId},
          'update': {
            'product_id': productId,
            'name': row.name,
            'slogan': row.slogan,
            'description': row.description,
            'category': row.category,
            'default_price': row.default_price,
          },
          'upsert': true,
        }
      }
      // Add it to the queue.
      operations.push(productOperation);
      if (operations.length > 100) {
        const tEnd = performance.now();
        console.log(`Bulk Update @ ${Math.round(tEnd - t0)}`);
        Product.bulkWrite(operations);
        operations = [];
      }
    })
    .on('end', (rowCount) => {
      Product.bulkWrite(operations);
      const tEnd = performance.now();
      console.log(`Added ${rowCount} products in ${Math.round(tEnd - t0)}`);
      resolve(rowCount);
    });

  });

};

const addProductFeatures = (csvPath) => {
  let operations = [];
  const t0 = performance.now();

  return new Promise((resolve, reject) => {
    createReadStream(path.resolve(__dirname, csvPath))
    .pipe(csv.parse({ headers: true }))
    .on('error', (error) => (reject(error)))
    .on('data', (row) => {
      const productId = parseInt(row.product_id, 10);
      const featuresOperation = {
        updateOne: {
          'filter': { 'product_id': productId },
          'update': {
            '$push': {
              'features': {
                'feature': row.feature,
                'value': row.value,
              }
            },
            'upsert': true
          },
        }
      };

      operations.push(featuresOperation);
      if (operations.length > 2500) {
        Product.bulkWrite(operations);
        const tEnd = performance.now();
        console.log(`Bulk Update @ ${Math.round(tEnd - t0)}`);
        operations = [];
      }
    })
    .on('end', (rowCount) => {
      Product.bulkWrite(operations);
      const tEnd = performance.now();
      console.log(`Added ${rowCount} product features in ${Math.round(tEnd - t0)}`)
      resolve(rowCount);
    })
  });
};

const addProductStyles = (csvPath) => {
  let operations = [];
  const t0 = performance.now();
  return new Promise((resolve, reject) => {
    createReadStream(path.resolve(__dirname, csvPath))
    .pipe(csv.parse({ headers: true }))
    .on('error', (error) => (reject(row.id)))
    .on('data', (row) => {
      const productId = parseInt(row.productId, 10);
      const stylesOperation = {
        updateOne: {
          'filter': {'product_id': productId },
          'update': {
            '$push': {
              'styles': {
                'style_id': parseInt(row.id, 10),
                'style_name': row.name,
                'sale_price': row.sale_price,
                'original_price': row.original_price,
                'default_style': row.default_style ? true : false,
              },
            }
          },
        }
      };

      operations.push(stylesOperation);

      if (operations.length > 2500) {
        Product.bulkWrite(operations);
        const tEnd = performance.now();
        console.log(`Bulk Update @ ${Math.round(tEnd - t0)}`);
        operations = [];
      }

    })
    .on('end', (rowCount) => {
      Product.bulkWrite(operations)
      const tEnd = performance.now();
      console.log(`Added ${rowCount} product styles in ${Math.round(tEnd - t0)}`)
      resolve(rowCount);
    })
  });
};

const addProductSkus = (csvPath) => {
  let operations = [];
  const t0 = performance.now();
  return new Promise((resolve, reject) => {
    createReadStream(path.resolve(__dirname, csvPath))
    .pipe(csv.parse({ headers: true }))
    .on('error', (error) => (reject(error)))
    .on('data', (row) => {

      const styleId = parseInt(row.styleId, 10);
      const skusOperation = {
        updateOne: {
          'filter': { 'styles.style_id': styleId },
          'update': {
            '$push': {
              'styles.$.skus': {
                'size': row.size ? row.size : '',
                'quantity': row.quantity ? parseInt(row.quantity, 10) : 0,
              }
            }
          },
        },
      };

      operations.push(skusOperation);
      if (operations.length > 2500) {
        Product.bulkWrite(operations);
        const tEnd = performance.now();
        console.log(`Bulk Update @ ${Math.round(tEnd - t0)}`);
        operations = [];
      }
    })
    .on('end', (rowCount) => {
      Product.bulkWrite(operations);
      const tEnd = performance.now();
      console.log(`Added ${rowCount} product skus in ${Math.round(tEnd - t0)}`)
      resolve(rowCount);
    })
  });
};

const addProductPhotos = (csvPath) => {
  let operations = [];
  const t0 = performance.now();
  return new Promise((resolve, reject) => {
    createReadStream(path.resolve(__dirname, csvPath))
    .pipe(csv.parse({ headers: true }))
    .on('error', (error) => (reject(error)))
    .on('data', (row) => {
      const photosOperation = {
        updateOne: {
          'filter': {'styles.style_id': row.styleId},
          'update': {
            '$push': {
              'styles.$.photos': {
                'url': row.url,
                'thumbnail_url': row.thumbnail_url,
              }
            }
          },
        },
      };

      operations.push(photosOperation);
      if (operations.length > 2500) {
        Product.bulkWrite(operations);
        const tEnd = performance.now();
        console.log(`Bulk Update @ ${Math.round(tEnd - t0)}`);
        operations = [];
      }
    })
    .on('end', (rowCount) => {
      Product.bulkWrite(operations);
      const tEnd = performance.now();
      console.log(`Added ${rowCount} product photos in ${Math.round(tEnd - t0)}`)
      resolve(rowCount);
    })
  });
};

module.exports = {
  addProducts,
  addProductFeatures,
  addProductStyles,
  addProductSkus,
  addProductPhotos,
};
