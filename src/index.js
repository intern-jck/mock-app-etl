const mongoose = require('mongoose');
const {addProducts, addFeatures, addStyles, addSkus, addPhotos} = require('./products/productsETL.js');
const {addQuestions, addAnswers, addAnswerPhotos} = require('./questions/questionsETL.js');
// const { addReviews, addPhotos, addCharacteristics, updateCharacteristics } = require('./reviews/reviewsETL.js');

// Location of raw csv data.
// Products
const productsCSV = '../../data/test/products/products10k.csv';
const featuresCSV = '../../data/test/products/features10k.csv';
const stylesCSV = '../../data/test/products/styles10k.csv';
const skusCSV = '../../data/test/products/skus10k.csv';
const productPhotosCSV = '../../data/test/products/photos10k.csv';

//Questions
const questionsCSV = '../../data/test/questions/questions10k.csv';
const answersCSV = '../../data/test/questions/answers10k.csv';
const answerPhotosCSV = '../../data/test/questions/answers_photos10k.csv';

//Related
const relatedCSV = '../../data/test/related/related10k.csv';

// Reviews
const reviewsCSV = '../data/test/reviews/reviews10k.csv';
const reviewPhotosCSV = '../data/test/reviews/reviews_photos10k.csv';
const characteristicsCSV = '../data/test/characteristics10k';
const characteristicReviewsCSV = '../data/test/characteristic_reviews10k';

// Connect to local db using user info.
mongoose.connect('mongodb://127.0.0.1:27017/mockapp',
  {
    useNewUrlParser: true,
    useUnifiedTopology: true,
  })
  .then(() => (console.log(`MongoDB Connected!`)))
  .catch((err) => (console.log(`MongoDB ERR ${err}`)));

// Build Products Collection
// addProducts(productsCSV)
// .then((rowCount) => {
//   console.log(`Added ${rowCount} Products`)
//   return addFeatures(featuresCSV);
// })
// .then((rowCount) => {
//   console.log(`Added ${rowCount} Features`)
//   return addStyles(stylesCSV);
// })
// .then((rowCount) => {
//   console.log(`Added ${rowCount} Styles`)
//   return addSkus(skusCSV);
// })
// .then((rowCount) => {
//   console.log(`Added ${rowCount} Skus`)
//   return addPhotos(productPhotosCSV);
// })
// .then((rowCount) => {
//   console.log(`Added ${rowCount} Photos`)
//   console.log('Products Collection Complete!');
// })
// .catch((error) => (console.log(error)));

// Build Questions Collection
addQuestions(questionsCSV)
.then((rowCount) => {
  console.log(`Added ${rowCount} Questions`)
  return addAnswers(answersCSV);
})
.then((rowCount) => {
  console.log(`Added ${rowCount} Answers`)
  return addAnswerPhotos(answerPhotosCSV);
})
.then((rowCount) => {
  console.log(`Added ${rowCount} Answer Photos`)
})
.catch((error) => (console.log(error)));

// Build Reviews Collection
// addReviews(reviewsCSV);
// addPhotos(photosCSV);
// addCharacteristics(characteristicsCSV);
// Used to add meta data values.
// updateCharacteristics(characteristicReviewsCSV);
