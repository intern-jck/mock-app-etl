const mongoose = require('mongoose');
const {Schema} = mongoose;

const AnswerModel = new Schema({
  answer_id: Number,
  body: String,
  date_written: Date,
  answerer_name: String,
  helpfulness: Number,
  reported: Boolean,
  photos: [{
    id: Number,
    url: String,
  }]

});

const Answer = mongoose.model('Answers', AnswerModel);
module.exports = Answer;
