const fs = require('fs');
const path = require('path');
const csv = require('fast-csv');
const mongoose = require('mongoose');
const Product = require('./ProductModel.js');

const addProducts = (csvPath) => {
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
        'filter': {'product_id': row.id},
        'update': {
          'product_id': row.id,
          'product_name': row.name,
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
    console.log(`Added ${rowCount} rows in ${Math.round(tEnd - t0)}`);
  });
};

const addFeatures = (csvPath) => {
  let operations = [];
  const t0 = performance.now();
  fs.createReadStream(path.resolve(__dirname, csvPath))
  .pipe(csv.parse({ headers: true }))
  .on('error', error => console.error(error))
  .on('data', (row) => {

    const featuresOperation = {
      updateOne: {
        'filter': { 'product_id': row.product_id },
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
    console.log(`Added ${rowCount} rows in ${Math.round(tEnd - t0)}`)
  });
};

const addStyles = (csvPath) => {
  let operations = [];
  const t0 = performance.now();

  fs.createReadStream(path.resolve(__dirname, csvPath))
  .pipe(csv.parse({ headers: true }))
  .on('error', error => console.error(error))
  .on('data', (row) => {

    const stylesOperation = {
      updateOne: {
        'filter': {'product_id': row.productId },
        'update': {
          '$push': {
            'styles': {
              'style_id': row.id,
              'style_name': row.name,
              'sale_price': row.sale_price,
              'original_price': row.original_price,
              'default_style': row.default_style,
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
    console.log(`Added ${rowCount} rows in ${Math.round(tEnd - t0)}`)
  });
};

const addSkus = (csvPath) => {
  let operations = [];
  const t0 = performance.now();
  fs.createReadStream(path.resolve(__dirname, csvPath))
  .pipe(csv.parse({ headers: true }))
  .on('error', error => console.error(error))
  .on('data', (row) => {
    const skusOperation = {
      updateOne: {
        'filter': { 'styles.style_id': row.styleId },
        'update': {
          '$push': {
            'styles.$.skus': {
              'size': row.size,
              'quantity': row.quantity,
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
    console.log(`Added ${rowCount} rows in ${Math.round(tEnd - t0)}`)
  });
};

const addPhotos = (csvPath) => {
  let operations = [];
  const t0 = performance.now();
  fs.createReadStream(path.resolve(__dirname, csvPath))
  .pipe(csv.parse({ headers: true }))
  .on('error', error => console.error(error))
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
    console.log(`Added ${rowCount} rows in ${Math.round(tEnd - t0)}`)
  });
};

module.exports = {
  addProducts,
  addFeatures,
  addStyles,
  addSkus,
  addPhotos,
};
