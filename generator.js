'use strict';

// Initialize libraries
const mgenerate = require("mgeneratejs");
const MongoClient = require('mongodb').MongoClient;
const fs = require('fs');

var d = new Date('2020-01-01 00:00:00');
var eventNumber = 0;
main().catch(console.error);

async function main() {

    //const uri = "ATLAS-URI";
    //const uri = "mongodb+srv://dbAdmin:Mongodb1@shared-demo.xhytd.mongodb.net/Veritas?retryWrites=true&w=majority";
    const uri = "mongodb://dbAdmin:Mongodb1@toms-mbp16.local:27017/Veritas?authSource=admin&authMechanism=SCRAM-SHA-256&readPreference=primary&ssl=false";
    const client = new MongoClient(uri, { useNewUrlParser: true, useUnifiedTopology: true });

    await client.connect();

    let rawTemplate = fs.readFileSync('template.json');
    let template = JSON.parse(rawTemplate);
    //console.log(template);

    const ndays = 3
    const devices = 3
    var deviceId;
    var day;

    const coll = client.db("Veritas").collection("ts_events");
    const filter = {};
    const result = await coll.deleteMany(filter);
    console.log("Deleted " + result.deletedCount + " documents");

    for ( day = 1; day <= ndays; day++) {
        for ( deviceId = 1; deviceId <= devices; deviceId++ ) {
            console.log(`On day ${day} of ${ndays}, ingesting DeviceId: ${deviceId}`);
            await insertEventsInBulk(deviceId, client, template)
                    .catch(console.error);
        }
    }   
    client.close();
}
  
async function insertEventsInBulk(deviceId, client, template) {
    
    const events = 24*240 // 240 events = 1 hour => 3600s / every 15 seconds)
    const coll = client.db("Veritas").collection("ts_events");
    const bulk = coll.initializeUnorderedBulkOp();
    const numMetrics = 10;

    var eventId;
    var metricId;

    for ( eventId = 1; eventId <= events; eventId++ ) {
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
        eventNumber = eventNumber + numMetrics;
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
