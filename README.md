To run this script you need nodejs installed

Then run (replacing the input and output paths):  
For fetching the postcode data
```sh
$ node --max-old-space-size=2048 postcodes.js INPUT_PATH.csv postcodes.json
```
Then, for converting the dataset
```sh
$ node --max-old-space-size=2048 convert.js INPUT_PATH.csv OUTPUT_PATH.csv
```

**Increase the heap size for larger datasets**
