'use strict'
require('dotenv').config();
const aws = require('aws-sdk');
const util = require('util');
const csvjson = require('csvjson');
const { google } = require('googleapis');
const {Storage} = require('@google-cloud/storage');
const storage = new Storage();
const bucket = storage.bucket('gs://pubsite_prod_rev_07641796729267858838');
const prefix = 'stats/installs/installs_com.affno.postalservicemobileapp';
const delimiter = '/';
const scopes = 'https://www.googleapis.com/auth/analytics.readonly';
const jwt = new google.auth.JWT(process.env.CLIENT_EMAIL, null, process.env.PRIVATE_KEY.replace(/\\n/gm, '\n'), scopes);
const view_id = '113507859';
let LocalDate = require("@js-joda/core").LocalDate;
const s3Bucket = process.env.S3_BUCKET;
const s3 = new aws.S3({region: 'eu-west-1'});

//function to put CSV reports in S3 bucket
function putObjectInS3Bucket(object){
    let jsonObject = JSON.parse(JSON.stringify(object));
    const csvData = csvjson.toCSV(jsonObject, {headers: "key"});
    const params = {
        Bucket: s3Bucket, // your bucket name
        Key: `user-data.csv`,
        ACL: 'public-read',
        Body: csvData,
        ContentType: 'text/csv',
    };
    s3.upload(params, (s3Err, data) => {
        if (s3Err) {
            throw s3Err;
        }
        return data;
    });

}
//function to pull data from google analytics, with predefined start date, end date, metrics and dimensions
async function getData(){
    const response = await jwt.authorize();
    const result = await google.analytics('v3').data.ga.get({
        'auth': jwt,
        'ids': 'ga:' + view_id,
        'start-date': '1095daysAgo', //three years ago
        'end-date': 'yesterday', //today's date
        'metrics': 'ga:28dayUsers,ga:sessions',
        'dimensions': 'ga:date',
        'max-results': 10000
    });
    return result;
}

//function to return files specifically for Bahrain Postal Services, category 'overview'
async function listFilesByPrefix(){
    const options = {
        prefix: prefix,
    };

    if (delimiter) {
        options.delimiter = delimiter;
    }
    let [files] = await bucket.getFiles(options);
    return files;

}
//function to read installs data in the csv file and return it as an array
async function getInstallData(files) {
    let installsArray = [];
    for (let fileElement of files) {
        const file = bucket.file(fileElement.name);
        let data = await file.download();
        const contents = data[0];
        let x = contents.toString('utf16le');
        let array = x.split('\n');
        array.shift(); array.pop();
        for (let arrayElement of array) {
            let tempArray = arrayElement.split(',');
            installsArray.push([LocalDate.parse(tempArray[0]), tempArray[9]]);
        }
    }
    return installsArray;
}
exports.handler = async (event, context, callback) => {
    try {
        let datesArray = [];
        let applicationName;
        let applicationDictionary = [];
        let results = await getData(); //call function to get data from google analytics
        applicationName = results.data.profileInfo.profileName; //extract the name of the application from the result JSON
        datesArray.push(results.data.rows); //push result data in dates array
        //flatten the array
        datesArray = datesArray.flat();

        let files = await listFilesByPrefix(); //call function to get files that have Install Events for specific application
        //structure dates from '20190101' to '2019-01-01' in datesArrayModified
        let datesArrayModified = datesArray.map(datesArrayItem => datesArrayItem[0]);
        //parse dates as Joda dates
        datesArrayModified = datesArrayModified.map(str => LocalDate.of(str.substring(0,4), str.substring(4,6), str.substring(6,8)) );
        let index = 0;
        console.log("datesArrayModified", datesArrayModified);
        let yearmonth = '';
        let installsArray = [];
        let filesForSpecifiedYearAndMonth = [];
        //loop through dates array and push data in application dictionary
        for(let datesArrayItem of datesArray){
            if(yearmonth === ''){
                yearmonth = datesArrayItem[0].substring(0,6); //get the year and month
                filesForSpecifiedYearAndMonth = files.filter(filesElement => filesElement.name.includes(yearmonth) && filesElement.name.includes('overview'));
                installsArray = await getInstallData(filesForSpecifiedYearAndMonth);
            }
            else {
                //get the installs data once for every month
                if(yearmonth !== datesArrayItem[0].substring(0,6)){
                    yearmonth = datesArrayItem[0].substring(0,6);
                    filesForSpecifiedYearAndMonth = files.filter(filesElement => filesElement.name.includes(yearmonth) && filesElement.name.includes('overview'));
                    installsArray = installsArray.concat(await getInstallData(filesForSpecifiedYearAndMonth));
                }
            }
            if(installsArray.length >0 && installsArray[index]!== undefined && installsArray.length >= index){
                if(datesArrayModified[index].equals(installsArray[index][0])){
                    applicationDictionary.push({
                        Name: applicationName,
                        Date: datesArrayModified[index],
                        Usage: datesArrayItem[2],
                        ActiveUsers: datesArrayItem[1],
                        Installs: installsArray[index][1]

                    });
                    index++;
                }
            }

        }
        console.log("installsArray", installsArray);
//        console.log("application dictionary");
//        console.log(util.inspect(applicationDictionary, false, null, true));
        let objectResult = putObjectInS3Bucket(applicationDictionary);

    } catch (err) {
        console.error(err);
    }
};
