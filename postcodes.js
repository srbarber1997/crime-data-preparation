const parse = require("csv-parse");
const stringify = require("csv-stringify");
const fs = require("fs");
const path = require("path");
const _ = require("lodash");
const moment = require("moment");
const fetch = require("node-fetch");

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
const calls = 0;
const callsCompleted;
const errors = [];

function receivePostcode(long, lat, postcode) {
  poscodes[long + ":" + lat] = postcode.result[0].outcode;
  calls--;
  callsCompleted++;
  updateCalls(false);
}

function requestPostcode(long, lat) {
  calls++;
  fetch(`https://api.postcodes.io/postcodes?lon=${long}&lat=${lat}`)
    .then(res => res.json())
    .then(postcode => receivePostcode(long, lat, postcode))
    .catch(e => {
      errors.push(e);
      calls--;
      updateCalls(false);
    });
  updateCalls(true);
}

function updateCalls(up) {
  process.stdout.clearLine();
  process.stdout.clearLine();
  process.stdout.cursorTo(0);
  process.stdout.write(`Completed calls: ${callsCompleted}\n`);
  process.stdout.write(`Ongoing calls: ${calls} ${up ? '+' : '-'}`);
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
    const body = output.slice(1);  
    body.map(row => {
        const obj = {};
        row.forEach(function(cell, index) {
          obj[header[index]] = cell;
        });
        return obj;
      })
      .forEach((row) => {
        const longitude = row["Longitude"];
        const latitude = row["Latitude"];
        if (longitude && latitude) {
          while (calls > 9) { }
          requestPostcode(longitude, latitude);
        }
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
