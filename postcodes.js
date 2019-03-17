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
    output = undefined;
    postcodes = {};
    callsToComplete = 0;
    body
      .map(row => {
        const obj = {};
        row.forEach(function(cell, index) {
          obj[header[index]] = cell;
        });
        return obj;
      })
      .forEach((row, index) => {
        const longitude = row["Longitude"];
        const latitude = row["Latitude"];
        if (longitude && latitude) {
          callsToComplete++;
          if (index > 0) {
            process.stdout.clearLine();
            process.stdout.cursorTo(0);
            process.stdout.write("Calls To Complete: " + callsToComplete);
          }
          fetch(
            "https://api.postcodes.io/postcodes?lon=" +
              longitude +
              "&lat=" +
              latitude
          )
            .then(res => {
              callsToComplete--;
              return res.json();
            })
            .then(postcode => {
              poscodes[longitude + ":" + latitude] = postcode.result[0].outcode;
            })
            .catch(e => {
              callsToComplete--;
            });
        }
      });
    while (callsToComplete > 0) {
      process.stdout.clearLine();
      process.stdout.cursorTo(0);
      process.stdout.write("Calls To Complete: " + callsToComplete);
    }
    outputData = JSON.stringify(postcodes);
    process.stdout.write("Writing to: " + outputFile + "\n");
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
