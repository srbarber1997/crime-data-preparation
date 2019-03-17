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

function converter(row) {
  // The row is an object such that:
  // {
  //    columnName: cellData,
  //    ...
  // }

  const longitude = row["Longitude"];
  const latitude = row["Latitude"];
  const postcode;

  const date = moment(row["Date"]);
  row["Day of Week"] = date.day();
  row["Is Weekend"] = date.day() === 0 || date.day() === 6;
  row["Is Weekday"] = date.day() !== 0 && date.day() !== 6;
  row["Hour"] = date.hour();

  const gender = row["Gender"];
  row["Is Male"] = gender.toLowerCase() === "male";
  row["Is Female"] = gender.toLowerCase() === "female";

  const type = row["Type"];
  row["Is Person Search"] =
    type.toLowerCase() === "person search" ||
    type.toLowerCase() === "person and vehicle search";
  row["Is Vehicle Search"] =
    type.toLowerCase() === "vehicle search" ||
    type.toLowerCase() === "person and vehicle search";

  const ageRange = row["Age Range"];
  const adultRanges = ["18-24", "25-34", "over 34"];
  const childRanges = ["10-17", "under 10"];
  row["Is adult"] = adultRanges.includes(ageRange);
  row["Is child"] = childRanges.includes(ageRange);

  const policingOp = row["Part of a policing operation"] || "";
  row["Part of a policing operation"] =
    policingOp.toLowerCase() === "true" ? "true" : "false";
  
  return row;
}

fs.readFile(file, "utf8", function(e, input) {
  if (e) {
    console.error(e);
    return;
  }
  process.stdout.write("Parsing data for conversion...\n");

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
    const header = output[0];
    const body = output.slice(1);
    output = undefined;

    count = 0;
    const newBody = _.flatten(
      _.chunk(body, Math.ceil(body.length / 100)).map(function(rows) {
        if (count > 0) {
          process.stdout.clearLine();
          process.stdout.cursorTo(0);
        }
        process.stdout.write("Converting... " + count + "%");
        count++;
        return rows.map(function(row) {
          const obj = {};
          row.forEach(function(cell, index) {
            obj[header[index]] = cell;
          });
          const convertedObj = converter(obj) || obj;
          const newRow = [];
          Object.keys(convertedObj).forEach(function(head) {
            if (!header.includes(head)) {
              process.stdout.clearLine();
              process.stdout.cursorTo(0);
              process.stdout.write("Included new column: " + head + "\n");
              process.stdout.write("Converting... " + count + "%");
              header.push(head);
            }
          });
          header.forEach(function(head) {
            newRow.push(String(convertedObj[head]));
          });
          return newRow;
        });
      })
    );

    process.stdout.clearLine();
    process.stdout.clearLine();
    process.stdout.cursorTo(0);
    process.stdout.write("Done. Processed " + newBody.length + " entries\n");

    let outputData = newBody;
    outputData.splice(0, 0, header);

    outputData = _.join(
      outputData.map(function(row) {
        return _.join(row, ",");
      }),
      "\r\n"
    );

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
