const mongoose = require('mongoose');
const {Schema} = mongoose;

const AnswerModel = new Schema({

});

const Answer = mongoose.model('Questions', AnswerModel);
module.exports = Answer;
