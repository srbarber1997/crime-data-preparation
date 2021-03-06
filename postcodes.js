const parse = require("csv-parse");
const stringify = require("csv-stringify");
const fs = require("fs");
const path = require("path");
const _ = require("lodash");
const moment = require("moment");
const fetch = require('node-fetch');

const file = path.resolve(__dirname, process.argv[2]);
const outputFile = path.resolve(__dirname, process.argv[3]);
if (!file) {
  throw new Error("No input file path given");
}
if (!outputFile) {
  throw new Error("No output file path given");
}
const startTime = moment().valueOf();
console.log("Reading input from: " + file);

const postcodes = fs.existsSync(outputFile) ? JSON.parse(fs.readFileSync(outputFile)) : {};
let calls = 0;
let callsCompleted = 0;
const errors = [];

function updateCalls() {
  process.stdout.clearLine();
  process.stdout.cursorTo(0);
  process.stdout.write(`Completed calls: ${callsCompleted}, `);
  process.stdout.write(`Ongoing calls: ${calls}, `);
  process.stdout.write(`Errors: ${errors.length}`);
}

function receivePostcode(long, lat, postcode) {
  // The postcode object from the postcodes.io url has an array of
  // postcodes. The first in the array is the smallest distance from
  // the longitude and latitude
  if (postcode.result === null) {
    postcodes[long + ":" + lat] = null;
  } else {
    postcodes[long + ":" + lat] = postcode.result[0];
  }
  if (callsCompleted % 1000 === 100) {
    fs.writeFileSync(outputFile, JSON.stringify(postcodes, null, 2));
  }
  calls--; callsCompleted++;
  updateCalls();
}

function requestPostcode(long, lat) {
  const URL = `https://api.postcodes.io/postcodes?lon=${long}&lat=${lat}`;
  let completed = false;
  calls++; fetch(URL)
    .then(res => {
      if (res.ok) {
        completed = true; 
        return res.json();
      }
      throw new Error('Non OK response: ' + res.status);
    })
    .then(postcode => {
      receivePostcode(long, lat, postcode)
    })
    .catch(e => {
      errors.push(e);
      calls--;
      updateCalls();
    });
  setTimeout(() => {
    if (!completed) {
      errors.push(`Timed out: ${URL}`);
      calls--;
      updateCalls();
    }
  }, 5000);
  updateCalls();
}

fs.readFile(file, "utf8", function(e, input) {
  if (e) {
    console.error(e);
    return;
  }
  process.stdout.write("Parsing data for fetching...\n");

  const parser = parse(input, {
    delimiter: ","
  });

  let output = [];

  parser.on("readable", function() {
    let record;
    while ((record = parser.read())) {
      output.push(record);
    }
  });

  parser.on("error", function(err) {
    console.error(err.message);
  });

  const fetchInterval = {};
  parser.on("end", function() {
    const header = output[0];
    const body = output.slice(1).splice(0, 2000)
    .map(row => {
        const obj = {};
        row.forEach(function(cell, index) {
          obj[header[index]] = cell;
        });
        return obj;
      })
      .filter(row => row["Longitude"] && row["Latitude"])
      .filter(row => !Object.keys(postcodes).includes(`${row["Longitude"]}:${row["Latitude"]}`))
      
    process.stdout.write("Fetching " + body.length + " Postcodes...\n");
    body.forEach(row => {
      const longitude = row["Longitude"];
      const latitude = row["Latitude"];

      fetchInterval[longitude + ":" + latitude] = setInterval(() => {
        if (calls < 500) {
          if (!Object.keys(postcodes).includes(`${longitude}:${latitude}`)) {
            requestPostcode(longitude, latitude);
          }
          clearInterval(fetchInterval[longitude + ":" + latitude]);
        }
      }, 100);
    });
    const finishInteval = setInterval(() => {
      if (calls > 0) {
        return;
      }

      process.stdout.write('\n');
      process.stdout.write(`Completed with ${errors.length} errors${errors.length === 0 ? ':' : ''}`);
      process.stdout.write('\n');
      errors.forEach(error => process.stdout.write(`\n - ${error}`));
      
      process.stdout.write("\nWriting to: " + outputFile + "\n");
      fs.writeFile(outputFile, JSON.stringify(postcodes, null, 2), function(e) {
        if (e) {
          console.error(e);
        } else {
          process.stdout.write("Output writen to: " + outputFile);
          process.stdout.write(
            "Completed in " + (moment().valueOf() - startTime) + "ms"
          );
        }
      });
      clearInterval(finishInteval);
    }, 5000);
  });
});
