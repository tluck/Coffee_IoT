'use strict';

// Initialize libraries
const mgenerate = require("mgeneratejs");
const MongoClient = require('mongodb').MongoClient;
const fs = require('fs');

// variables
var d = new Date('2020-01-01 00:00:00');
const numDays = 90
const numDevices = 3
const numMetrics = 10
const numEvents = 24*3600/15 // daily events = 24 hours a day every 15 seconds

//const uri = "mongodb+srv://dbAdmin:Mongodb1@shared-demo.xhytd.mongodb.net/Veritas?retryWrites=true&w=majority";
//const uri = "mongodb://dbAdmin:Mongodb1@toms-mbp16.local:27017/Veritas?authSource=admin&authMechanism=SCRAM-SHA-256&readPreference=primary&ssl=false";
const uri = "mongodb+srv://dbAdmin:Mongodb1@time-series.qnuuu.mongodb.net/test";
const client = new MongoClient(uri, { useNewUrlParser: true, useUnifiedTopology: true });
const dbName = "Veritas";
const newCollection = "ts_collection";
//const newCollection = "ts_events";
const db = client.db(dbName);
const coll = client.db(dbName).collection(newCollection);

var eventNumber = 0;
main().catch(console.error);

async function main() {

    await client.connect();

    let rawTemplate = fs.readFileSync('template.json');
    let template = JSON.parse(rawTemplate);
    console.log(`Using: ${dbName}.${newCollection}`);

    var day;
    const filter = {};
    var result = await coll.deleteMany(filter);
    console.log("Deleted " + result.deletedCount + " documents");

    var idx = await db.command({listIndexes: newCollection});
    console.log(`Indexes: ${idx.cursor.ns}`);
    if ( idx.cursor.firstBatch.length > 0 ) {
        result = await coll.dropIndex( idx.cursor.firstBatch[0].name ).catch(console.error);
        console.log(`Deleted Index: ${result.ok}`);
    }
    result = await coll.createIndex({ ts: 1 });
    console.log(`Created Index: ${result}`);

    for ( day = 1; day <= numDays; day++) {
            console.log(`On day ${day} of ${numDays}`);
            await insertEventsInBulk(template)
                    .catch(console.error);
    }   
    client.close();
}
  
async function insertEventsInBulk(template) {

    const bulk = coll.initializeOrderedBulkOp();

    var eventId;
    var deviceId;
    var metricId;
    for ( eventId = 1; eventId <= numEvents; eventId++ ) {
        for ( deviceId = 1; deviceId <= numDevices; deviceId++ ) {
            for ( metricId = 1; metricId <= numMetrics; metricId++ ) {
                // Overwrite with specific values
                // ==============================
                
                // Seed template with specific values
                var units = [getRndInt(1000, 9999), getRndInt(5000, 7999), getRndInt(10, 100), getRndInt(50, 80)];

                template.ts = d;
                template.metric_name = `metric${metricId}`;
                template.metadata.appliance_id = `ABC00000${deviceId}`;
                template.value = getRndInt(100000, 4999998)+" "+units[0]+" "+units[1]+" "+units[2]+" "+units[3];

                // Add new field to template at run time
                //template.donsField = deviceId + " for Don";

                // Generate event with random data
                let event = mgenerate(template);
                
                // ==============================
                // Insert
                bulk.insert(event);
            }
        }
        eventNumber = eventNumber + numMetrics*numDevices;
        d = new Date(d.getTime() + 15000);
    }
    
    console.log(`Date: ${d} Events: ${eventNumber}`);       
    await bulk.execute();
    //}
}

function getRndInt(min, max) {
    return mgenerate(
        {
            aNumber: {"$natural":{"min":min, "max":max}}
        }
    ).aNumber;
}
