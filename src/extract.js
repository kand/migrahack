'use strict';

let lodash = require('lodash');
let AdmZip = require('adm-zip');
let parse = require('csv-parse');
var stringify = require('csv-stringify');
var fs = require('fs');
let StringDecoder = require('string_decoder').StringDecoder;

let datas = {};

let colFilter = (col) => {
  return lodash.pullAt(col,
    0, 1,   // ids
    2,      // tract name
    343,    // total pop
    371, 372, 373, 374, 375, 376, 378, 379, 380, 381, 382 // citizenship status
  );
};

module.exports = {
  getData () {
    let zip2010 = new AdmZip(__dirname + '/data/ACS_10_5YR_DP02.zip');
    let zip2011 = new AdmZip(__dirname + '/data/ACS_11_5YR_DP02.zip');
    let zip2012 = new AdmZip(__dirname + '/data/ACS_12_5YR_DP02.zip');
    let zip2013 = new AdmZip(__dirname + '/data/ACS_13_5YR_DP02.zip');
    let zip2014 = new AdmZip(__dirname + '/data/ACS_14_5YR_DP02.zip');

    let csvs = [
      zip2010.getEntries().filter((entry) => { return entry.name === 'ACS_10_5YR_DP02.csv'; })[0],
      zip2011.getEntries().filter((entry) => { return entry.name === 'ACS_11_5YR_DP02.csv'; })[0],
      zip2012.getEntries().filter((entry) => { return entry.name === 'ACS_12_5YR_DP02.csv'; })[0],
      zip2013.getEntries().filter((entry) => { return entry.name === 'ACS_13_5YR_DP02.csv'; })[0],
      zip2014.getEntries().filter((entry) => { return entry.name === 'ACS_14_5YR_DP02.csv'; })[0],
    ];

    let done = lodash.after(csvs.length, () => {
      console.log('done loading data.');
    });

    console.log('loading data...');
    csvs.forEach((entry) => {
      const decoder = new StringDecoder('utf-8');
      parse(decoder.write(entry.getData()), {}, (err, rows) => {
        let name = entry.name.match(/ACS_(\d\d)_5YR/)[1];

        datas[name] = {
          rows: rows.map((row) => {
            return colFilter(row);
          })
        };

        done();
      });
    });

    return datas;
  },
  writeOutFilteredData () {
    let dir = `${__dirname}/output`;
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir);
    }

    Object.keys(datas).forEach((year) => {
      stringify(datas[year].rows, (err, output) => {
        fs.writeFile(`${dir}/${year}.csv`, output);
      });
    });
  },
  writeOutForeignBornRates () {
    let dir = `${__dirname}/output`;
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir);
    }

    let rates = this.calculateForeignBornRates();
    let cols = Object.keys(rates[Object.keys(rates)[0]]).sort();
    let rows = [['tract'].concat(cols)]
      .concat(
        Object.keys(rates)
          .reduce((rows, tract) => {
            let row = [tract];

            cols.forEach((col) => row.push(rates[tract][col]));

            rows.push(row);
            return rows;
          }, [])
      );

    stringify(rows, (err, output) => {
      fs.writeFile(`${dir}/rates.csv`, output);
    });
  },
  calculateForeignBornRates () {
    return Object.keys(datas)
      .map((yearStr) => Number(yearStr))
      .sort()
      .reduce((tracts, yr) => {
        datas[yr].rows
          .filter((row, i) => i > 1)
          .forEach((row) => {
            let lastYr = Number(yr) - 1;
            let name = row[2];
            let totalPop = Number(row[3]);
            let foreignBornPop = Number(row[4]);
            let yrPercent = foreignBornPop / totalPop;

            if (lodash.isUndefined(tracts[name])) {
              tracts[name] = {
                id: row[0],
                id2: row[1]
              };
            } else {
              tracts[name][`${lastYr}-${yr}`] = tracts[name][lastYr] - yrPercent;
            }

            tracts[name][yr] = yrPercent;
          });

        return tracts;
      }, {});
  }
};
