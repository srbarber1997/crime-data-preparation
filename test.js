const fetch = require('node-fetch');

fetch('https://api.postcodes.io/postcodes?lon=-2.599742&lat=51.454085')
.then(res => res.json()) 
.then(postcode => console.log(postcode.result[0].postcode));