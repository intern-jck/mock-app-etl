const {Schema, model} = require('mongoose');

const ReviewSchema = new Schema({
  product_id: Number,
  results: [{
    review_id:  Number,
    rating:  Number,
    date: Date,
    summary: String,
    body: String,
    reviewer_name: String,
    reviewer_email: String,
    response: String,
    helpfulness: Number,
    recommend: Boolean,
    reported: Boolean,
    photos: [{ id: Number, url: String }],
  }],
  meta: {
    ratings: {
      1: Number,
      2: Number,
      3: Number,
      4: Number,
      5: Number
    },
    recommended: {
      0: Number,
      1: Number
    },
    characteristics: {}
  }
});

const Review = model('Reviews', ReviewSchema);
module.exports = Review;
