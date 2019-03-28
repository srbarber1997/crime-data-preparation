// Dependencies
const parse = require("csv-parse");
const fs = require("fs");
const path = require("path");
const _ = require("lodash");
const moment = require("moment");

// File paramerters for read and writing
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
const postcodes = fs.existsSync('postcodes.json') ? JSON.parse(fs.readFileSync('postcodes.json')) : {};

// Converter function to be called later (per row of the data)
function converter(row) {
  // The row is an object such that:
  // {
  //    columnName: cellData,
  //    ...
  // }

  // Get the date field as a moment date
  const date = moment(row["Date"]);

  // Create new columns for date based data
  row["Day of Week"] = date.day();
  row["Is Weekend"] = date.day() === 0 || date.day() === 6 ? 1 : 0;
  row["Is Weekday"] = date.day() !== 0 && date.day() !== 6 ? 1 : 0;
  row["Hour"] = date.hour();

  // Get the gender field
  const gender = row["Gender"];

  // Create new columns based of the gender field
  row["Is Male"] = gender.toLowerCase() === "male" ? 1 : 0;
  row["Is Female"] = gender.toLowerCase() === "female" ? 1 : 0;

  // Get Type of search field
  const type = row["Type"];

  // Create fields for both types of search
  row["Is Person Search"] =
    type.toLowerCase() === "person search" ||
    type.toLowerCase() === "person and vehicle search"  ? 1 : 0;
  row["Is Vehicle Search"] =
    type.toLowerCase() === "vehicle search" ||
    type.toLowerCase() === "person and vehicle search" ? 1 : 0;

  // Get age range field
  const ageRange = row["Age range"];

  // Set adult and child ranges
  const adultRanges = ["18-24", "25-34", "over 34"];
  const childRanges = ["10-17", "under 10"];

  // Create fields for adult and child searches 
  row["Is adult"] = adultRanges.includes(ageRange) ? 1 : 0;
  row["Is child"] = childRanges.includes(ageRange) ? 1 : 0;

  // Fill in missing data with false
  const policingOp = row["Part of a policing operation"] || "";
  row["Part of a policing operation"] =
    policingOp.toLowerCase() === "true" ? 1 : 0;

  const longitude = row["Longitude"];
  const latitude = row["Latitude"];
  if (postcodes[`${longitude}:${latitude}`]) {
    row['Postcode'] = postcodes[`${longitude}:${latitude}`].postcode || null;
    row['Outcode'] = postcodes[`${longitude}:${latitude}`].outcode || null;
    row['Incode'] = postcodes[`${longitude}:${latitude}`].incode || null;
  }

  const outcome = row['Outcome'];
  const notFoundOutcomes = [
    'Nothing found - no further action',
    'A no further action disposal'
  ];
  row['Item found']= !notFoundOutcomes.includes(outcome);
  return row;
}

// Reads the input file in csv format
fs.readFile(file, "utf8", function(e, input) {
  // Check for error with reading
  if (e) {
    console.error(e);
    return;
  }
  process.stdout.write("Parsing data for conversion...\n");

  // Parse the data by seperating it using commas
  const parser = parse(input, {
    delimiter: ","
  });

  // The output 2D array
  let output = [];

  // When the parser can read a value
  parser.on("readable", function() {
    let record;
    while ((record = parser.read())) {
      output.push(record);
    }
  });

  // When the parser hits an error
  parser.on("error", function(err) {
    console.error(err.message);
  });

  // When the parser finishes
  parser.on("end", function() {
    // Get the header from the output
    const header = output[0];

    // Get the body from the output
    const body = output.slice(1);

    // Set the count to 0% and split the body into 100 chunks (1 chunk = 1%)
    count = 0;
    const newBody = _.flatten(
      _.chunk(body, Math.ceil(body.length / 100)).map(function(rows) {
        // Log out the percentage
        if (count > 0) {
          process.stdout.clearLine();
          process.stdout.cursorTo(0);
        }
        process.stdout.write("Converting... " + count + "%");
        count++;

        // Convert the rows in the chunk
        return rows.map(function(row) {
          // Map the array row into an object row
          const obj = {};
          row.forEach(function(cell, index) {
            obj[header[index]] = cell;
          });
          return obj;
        })
        .map(obj => {
          const newObj = {};
          Object.keys(obj).forEach(key => {
            newObj[key] = obj[key]
            .replace('\r', '')
            .replace('\n', '');
          });
          return newObj;
        })
        .map(obj => {
          // Convert the object row
          const convertedObj = converter(obj) || obj;
          Object.keys(convertedObj).forEach(function(head) {
            // Add any new headers to the header array
            if (!header.includes(head)) {
              process.stdout.clearLine();
              process.stdout.cursorTo(0);
              process.stdout.write("Included new column: " + head + "\n");
              process.stdout.write("Converting... " + count + "%");
              header.push(head);
            }
          });

          // Return the new data in the order of the headers
          return header.map(function(head) {
            return convertedObj[head] || null;
          });
        });
      })
    );

    process.stdout.clearLine();
    process.stdout.clearLine();
    process.stdout.cursorTo(0);
    process.stdout.write("Done. Processed " + newBody.length + " entries\n");

    // Add the header row back to the data ready for convertion back to csv
    let outputData = newBody;
    outputData.splice(0, 0, header);

    // Convert to csv
    outputData = _.join(
      outputData.map(function(row) {
        return _.join(row, ",");
      }),
      "\r"
    );

    // Write data to output file
    process.stdout.write("Writing to: " + outputFile + "\n");
    fs.writeFile(outputFile, outputData, {}, function(e) {
      if (e) {
        console.error(e);
      } else {
        process.stdout.write("Output writen to: " + outputFile);
        process.stdout.write(
          "\nCompleted in " + (moment().valueOf() - startTime) + "ms"
        );
      }
    });
  });
});
