#!/usr/bin/env node
var numCPUs = require('os').cpus().length
var cluster = require('cluster');

var fs = require("fs");
var map = require('map-stream');
var ug = require("./lib/usergrid.js");
var jsonstream = require('./lib/jsonstream');

var argv = require('optimist')
    .usage('Usage: $0 -o yourorgname -a sandbox -f filename\nOPTIONAL:\n\t-authtype CLIENT_ID\n\t-clientId YOURCLIENTID\n\t-clientSecret YOURCLIENTSECRET\n\t-logging false\n\t-buildCurl false')
    .demand(['orgname', 'appname'])
    .options('file', {
        alias: 'f',
        describe: 'File containing a JSON array of entities to load'
    })
    .options('encoding', {
        alias: 'e',
        default: 'UTF-8',
        describe: 'File encoding'
    })
    .options('apiurl', {
        alias: 'p',
        default: 'https://api.usergrid.com',
        describe: "Your API Backend URL"
    })
    .options('authtype', {
        alias: 't',
        default: 'NONE',
        describe: "Authentications type. values are 'APP_USER' or 'CLIENT_ID'"
    })
    .options('clientId', {
        alias: 'i',
        default: undefined,
        describe: "your client ID for CLIENT_ID authentication"
    })
    .options('clientSecret', {
        alias: 's',
        default: undefined,
        describe: "your client secret for CLIENT_ID authentication"
    })
    .options('logging', {
        alias: 'l',
        default: false,
        describe: 'boolean describing whether App Services Logging should be on or off'
    })
    .options('buildCurl', {
        alias: 'b',
        default: false,
        describe: 'build an a curl command for calls and output them to the console'
    })
    .options('orgname', {
        alias: 'o',
        default: false,
        describe: 'Your organization name'
    })
    .options('appname', {
        alias: 'a',
        default: false,
        describe: 'Your application name'
    })
    .options('batchsize', {
        default: 1000,
        describe: 'the number of items to send at a given time'
    })
    .options('type', {
        default: null,
        describe: 'only accept entities of a certain type so they can be batched'
    })
    .check(function(args) {
        //console.log("arguments",JSON.stringify(arguments));
        if (args.authtype === 'CLIENT_ID') {
            if (!(args.i && args.s)) {
                throw "if CLIENT_ID auth is specified, then clientId and clientSecret are required"
            }
        }
    })
    .argv;

// console.log(argv);
// process.exit();
function getClient(callback) {
    var options = {
        URI:argv.apiurl,
        orgName: argv.o,
        appName: argv.a,
        authType: argv.authtype,
        logging: argv.l,
        buildCurl: argv.b
    };
    if (argv.authtype === 'CLIENT_ID') {
        options.clientId = argv.clientId;
        options.clientSecret = argv.clientSecret;
    }
    global._client = global._client || new ug.client(options, callback);
    return global._client;
}

function isUUID(uuid) {
    var uuidValueRegex = /^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/;
    if (!uuid) {
        return false;
    }
    return uuidValueRegex.test(uuid);
}

function save() {
    var map = require('map-stream');
    var client = getClient();
    var count = errors = waiting = 0;
    return map(function(data, callback) {
        try {
            var endpoint = data.type;
            if (!endpoint) {
                callback();
                return;
            }
            var method = "POST";
            if (isUUID(data.uuid)) {
                method = "PUT";
                endpoint += "/" + data.uuid
            }
            client.request({method:method,endpoint:endpoint,body:data}, function(err, data) {
                count++;
                if (err) {
                    console.error("Usergrid error: ", data);
                }
                // console.log(JSON.stringify(data,null,2));
                callback();
            });
        } catch (e) {
            count++;
            callback(false);
            console.error("Mapping error:", e);
        }
    });
}
var nodes=[];
function distribute() {
    var map = require('map-stream');
    var count = errors = waiting = 0;
    var nodes=Object.keys(cluster.workers);
    process.on('exit', function() {
        console.error("\n%d entities distributed\n", count);
    });
    cluster.on('disconnect', function(worker) {
        //nodes.push(cluster.fork());
    });
    return map(function(data, callback) {
        try{
            count++;
            var workerId=nodes.shift();
            cluster.workers[workerId].counter++;
            var worker=cluster.workers[workerId];
            worker.send(data);
            nodes.push(workerId);
            callback();
        }catch(e){
            callback();
            console.error(e);
        }
    });
}
if (cluster.isMaster) {
var client=getClient(function(){console.log(this.token, this);});
    var stream = (argv.file) ? fs.createReadStream(argv.file) : process.stdin;
    var output = (argv.out) ? fs.createWriteStream(argv.out) : process.stdout;
    var count = errors = waiting = 0;
    process.on('exit', function() {
        console.error("\n%d entities processed\n", count);
    });
    Array.apply(0, Array(argv.workers||numCPUs)).forEach(function(n, i) {
        var worker=cluster.fork();
        cluster.workers[worker.id].counter=0;
        worker.on('message', function(data) {
            count++;
            if(data!==null){
                console.log(JSON.stringify(data));
            }
            if(j._readableState.ended && (--cluster.workers[worker.id].counter)===0){
                //You may be excused.
                worker.disconnect();
            }
        });
    })
    //cluster.disconnect(function(){process.exit();})
    var j= jsonstream();
    var d=distribute();
    stream
        .pipe(j)
        .pipe(d)
        .pipe(output)

} else {
    var count = errors = waiting = 0;
    var client=getClient();
    cluster.worker.counter=0;
    process.on('message', function(data) {
        try {
            // console.log(cluster.worker.counter++);
            var endpoint = data.type;
            if (!endpoint) {
                return;
            }
            var method = "POST";
            if (isUUID(data.uuid)) {
                method = "PUT";
                endpoint += "/" + data.uuid
            }
            client.request({method:method,endpoint:endpoint,data:data}, function(err, data) {
                count++;
                if (err) {
                    console.error("Usergrid error: ", data);
                }
                cluster.worker.counter--;
                process.send(data);
            });
        } catch (e) {
            console.error("Mapping error:", e);
        }
    });
}

process.stdout.on('error', process.exit);

