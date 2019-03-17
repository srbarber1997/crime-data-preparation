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

const poscodes = {};
let calls = 0;
let callsCompleted = 0;
const errors = [];

function receivePostcode(long, lat, postcode) {
  poscodes[long + ":" + lat] = postcode.result[0].outcode;
  calls--; callsCompleted++;
  updateCalls();
}

function requestPostcode(long, lat) {
  const URL = `https://api.postcodes.io/postcodes?lon=${long}&lat=${lat}`;
  let completed = false;
  calls++;
  fetch(URL)
    .then(res => {
      if (res.ok) {
        completed = true; 
        return res.json();
      }
      throw new Error('Non OK response: ' + res.status);
    })
    .then(postcode => receivePostcode(long, lat, postcode))
    .catch(e => {
      errors.push(e);
      calls--;
      updateCalls();
    });
  setTimeout(() => {
    if (!completed) {
      erros.push(`Timed out: ${URL}`);
      calls--;
      updateCalls();
    }
  }, 500);
  updateCalls();
}

function updateCalls() {
  process.stdout.clearLine();
  process.stdout.cursorTo(0);
  process.stdout.write(`Completed calls: ${callsCompleted}, `);
  process.stdout.write(`Ongoing calls: ${calls}`);
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

  parser.on("end", function() {
    process.stdout.write("Fetching Postcodes...\n");
    const header = output[0];
    const body = output.slice(1).slice(0, 10); // Second .slice() to limit the sample to 10 rows for testing
    body.map(row => {
        const obj = {};
        row.forEach(function(cell, index) {
          obj[header[index]] = cell;
        });
        return obj;
      })
      .filter(row => row["Longitude"] && row["Latitude"])
      .forEach((row) => {
        const longitude = row["Longitude"];
        const latitude = row["Latitude"];

        while (calls > 9) { }
        requestPostcode(longitude, latitude);
      });
    while (calls > 0) { }
    process.stdout.write('\n');
    process.stdout.write(`Completed with {${erros.length} errors:\n`);
    process.stdout.write(errors);

    outputData = JSON.stringify(postcodes);
    process.stdout.write("\nWriting to: " + outputFile + "\n");
    fs.writeFile(outputFile, outputData, {}, function(e) {
      if (e) {
        console.error(e);
      } else {
        process.stdout.write("Output writen to: " + outputFile);
        process.stdout.write(
          "Completed in " + (moment().valueOf() - startTime) + "ms"
        );
      }
    });
  });
});
