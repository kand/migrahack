'use strict';

let lodash = require('lodash');
let AdmZip = require('adm-zip');
let parse = require('csv-parse');
var stringify = require('csv-stringify');
var fs = require('fs');
let StringDecoder = require('string_decoder').StringDecoder;

let datas;
let urbanTractIds;
let within60MilesTractIds;

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
    if (datas) {
      return datas;
    } else {
      datas = {
        meta: {},
        years: {}
      };
    }

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
      __dirname + '/data/IL_URBAN.csv',
      __dirname + '/data/IL_WITHIN_60_MILES.csv'
    ];

    let done = lodash.after(csvs.length, () => {
      console.log('done loading data.');
    });

    console.log('loading data...');
    csvs.forEach((entry) => {
      const decoder = new StringDecoder('utf-8');
      let raw;
      if (typeof entry.getData !== 'undefined') {
        raw = decoder.write(entry.getData());
      } else {
        raw = fs.readFileSync(entry, 'utf-8')
      }

      parse(raw, {}, (err, rows) => {
        if (entry.name) {
          let name = entry.name.match(/ACS_(\d\d)_5YR/)[1];

          datas.years[name] = {
            rows: rows.map((row) => {
              return colFilter(row);
            })
          };
        } else {
          let name = entry.split('/');
          datas.meta[name[name.length - 1].replace('.csv', '')] = rows;
        }

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

    Object.keys(datas.years).forEach((year) => {
      stringify(datas.years[year].rows, (err, output) => {
        fs.writeFile(`${dir}/${year}.csv`, output);
      });
    });
  },
  writeOutForeignBornRates () {
    let dir = `${__dirname}/output`;
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir);
    }

    let rates = this.calculateForeignBornRatesByTract();
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
  calculateForeignBornRatesOutside60Miles () {
    if (!within60MilesTractIds) {
      within60MilesTractIds = datas.meta['IL_WITHIN_60_MILES'].map((row) => row[0]);
    }

    return this.calculateForeignBornRatePerYr((row) => {
      return within60MilesTractIds.indexOf(row[1]) === -1;
    });
  },
  calculateForeignBornRatePerYrRural () {
    if (!urbanTractIds) {
      urbanTractIds = datas.meta['IL_URBAN'].map((row) => row[2]);
    }

    return this.calculateForeignBornRatePerYr((row) => {
      return urbanTractIds.indexOf(row[0]) === -1;
    });
  },
  calculateForeignBornRatePerYrUrban () {
    if (!urbanTractIds) {
      urbanTractIds = datas.meta['IL_URBAN'].map((row) => row[2]);
    }

    return this.calculateForeignBornRatePerYr((row) => {
      return urbanTractIds.indexOf(row[0]) > -1;
    });
  },
  calculateForeignBornRatePerYr (addFilter) {
    let useFilter = addFilter ? addFilter : (row) => true;

    return Object.keys(datas.years)
      .map((yearStr) => Number(yearStr))
      .sort()
      .reduce((yrs, yr) => {
        yrs[yr] = datas.years[yr].rows
          .filter((row, i) => i > 1 && useFilter(row))
          .map((row) => {
            return {
              total: Number(row[3]),
              foreign: Number(row[4])
            };
          })
          .reduce((totals, mapped) => {
            return {
              total: totals.total + mapped.total,
              foreign: totals.foreign + mapped.foreign
            };
          }, { total: 0, foreign: 0 });
        return yrs;
      }, {})
  },
  calculateForeignBornRatesByTract () {
    return Object.keys(datas.years)
      .map((yearStr) => Number(yearStr))
      .sort()
      .reduce((tracts, yr) => {
        datas.years[yr].rows
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
