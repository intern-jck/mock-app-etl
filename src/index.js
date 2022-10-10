const mongoose = require('mongoose');
const {addProducts, addFeatures, addStyles, addSkus, addPhotos} = require('./products/productsETL.js');
// const { addReviews, addPhotos, addCharacteristics, updateCharacteristics } = require('./reviews/reviewsETL.js');

// Location of raw csv data.
// Products
const productsCSV = '../../data/test/products/products10k.csv';
const featuresCSV = '../../data/test/products/features10k.csv';
const stylesCSV = '../../data/test/products/styles10k.csv';
const skusCSV = '../../data/test/products/skus10k.csv';
const photosCSV = '../../data/test/products/photos10k.csv';

//Questions

//Related

// Reviews
// Location of raw csv data.
// const reviewsCSV = '../data/test/products/reviews/reviews10k.csv';
// const photosCSV = '../data/test/products/reviews/reviews_photos10k.csv';
// const characteristicsCSV = '../data/test/products/characteristics10k';
// const characteristicReviewsCSV = '../data/test/products/characteristic_reviews10k';

//Cart

// Connect to local db using user info.
mongoose.connect('mongodb://127.0.0.1:27017/sdc',
  {
    useNewUrlParser: true,
    useUnifiedTopology: true,
  })
  .then(() => (console.log(`MongoDB Connected!`)))
  .catch((err) => (console.log(`MongoDB ERR ${err}`)));

  // Build Productions Collection
// addProducts(productsCSV);
// addFeatures(featuresCSV);
// addStyles(stylesCSV);
// addSkus(skusCSV);
// addPhotos(photosCSV);

// Build Reviews Collection
// addReviews(reviewsCSV);
// addPhotos(photosCSV);
// addCharacteristics(characteristicsCSV);
// Used to add meta data values.
// updateCharacteristics(characteristicReviewsCSV);
