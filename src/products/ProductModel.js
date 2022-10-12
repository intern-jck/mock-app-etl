const {Schema, model} = require('mongoose');

const ProductModel = new Schema({
  product_id: Number,
  name: String,
  slogan: String,
  description: String,
  category: String,
  default_price: String,
  features:[
    {
      feature: String,
      value: String,
    }
  ],
  styles: [
    {
      style_id: Number,
      style_name: String,
      sale_price: String,
      original_price: String,
      default_style: Boolean,
      skus: [
        {
          size: String,
          quantity: Number,
        }
      ],
      photos: [
        {
          url: String,
          thumbnail_url: String,
        }
      ]
    }
  ]
});

const Product = model('Products', ProductModel);
module.exports = Product;
