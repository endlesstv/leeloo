/**
 * "Multi-pass!"
 * "Yes, dear, she knows it's a multipass."
 */
var events = require("events"),
	net = require("net"),
	util = require("util"),
	Db = require("mongodb").Db,
	Server = require("mongodb").Server,
	ReplSetServers = require("mongodb").ReplSetServers;
	
var Leeloo = function() {
	events.EventEmitter.call(this);
}

util.inherits(Leeloo, events.EventEmitter);


/**
 * Prints a help message.
 *
 * @param {String} details Receive detailed information about this command [optional]
 * @param {Function} callback `callback(error)` [optional]
 */
Leeloo.prototype.help = function(details, callback) {
	var self = this;
	
	if (details instanceof Function) {
		callback = details;
		details = null;
	}
	
	details = details || "";
	
	switch (details) {
		default:
			console.log("usage: leeloo [command] -options\n");	
			console.log("commands:");
			console.log("\thelp");
			console.log("\tlist");
			console.log("\tschedule");
			console.log("\tstart-server");
			console.log("\tstatus");			
			console.log("\tstop-server");
	}
	
	callback && callback();
}


/**
 * Opens the MongoDB database.
 *
 * @param {Function} callback `callback(error)`
 */
Leeloo.prototype._openDatabase = function(callback) {
	var self = this,
		context = require("../conf/env.json");
	
	self._context = context;
		
	if (self._context.mongodb && self._context.mongodb.using_replica_sets) {
		// Set up the MongoDB replica set servers
		var replicaSet = new ReplSetServers([
				new Server(self._context.mongodb.servers[0].host, self._context.mongodb.servers[0].port, {"auto_reconnect": true}),
				new Server(self._context.mongodb.servers[1].host, self._context.mongodb.servers[1].port, {"auto_reconnect": true}),
				new Server(self._context.mongodb.servers[2].host, self._context.mongodb.servers[2].port, {"auto_reconnect": true})				
			],
			{"rs_name": self._context.mongodb.replica_set_name}
		);

		// Connect to our databases and open
		self._store = new Db(self._context.mongodb.datastore, replicaSet);
	}
	else {
		// Connect to our database
		self._store = new Db(self._context.mongodb.datastore, new Server(self._context.mongodb.servers[0].host, self._context.mongodb.servers[0].port, {"auto_reconnect": true}));
	}

	self._store.open(callback);	
}


/**
 * Lists jobs.
 *
 * @param {Function} callback `callback(error)` [optional]
 */
Leeloo.prototype.list = function(callback) {
	var self = this;
	
	self._store.collection(self._context.mongodb.collection, function(error, collection) {
		collection.find({"s": "s"}, {}, {}, function(error, cursor) {
			cursor.toArray(function(error, array) {
				console.log("listed jobs");
				callback && callback(error, {"jobs": array});	
			});
		});
	});
}


/**
 * Schedules a job.
 *
 * @param {Object} params Additional parameters supplied by the scheduler
 * @param {Integer} params.when At what time (ms) should we schedule this job?
 * @param {String} params.type What type of job is this?
 * @param {Boolean} params.reschedule At what time (ms) should we reschedule? [optional]
 * @param {Function} callback `callback(error)` [optional]
 */
Leeloo.prototype.schedule = function(params, callback) {
	var self = this,
		job = {};
	
	if (params instanceof Function) {
		callback = params;
		params = null;
	}
	
	params = params || {};
	job.w = params.when || Date.now() + 600000;
	job.t = params.type || "u";
	job.r = params.reschedule || 0;
	job.s = "s";
	
	self._store.collection(self._context.mongodb.collection, function(error, collection) {
		collection.insert(job, {"safe": true}, function(error, documents) {
			console.log("scheduled a job");
			callback && callback();
		});
	});
}


/**
 * Starts Leeloo.
 *
 * @param {Function} callback `callback(error)` [optional]
 */
Leeloo.prototype.startServer = function(callback) {
	var self = this;
	
	self._openDatabase(function(error) {
		self._server = net.createServer({"allowHalfOpen": true}, function(socket) {
			socket.setEncoding("utf8");
			var data = "";
	
			// A client is connecting
			socket.on("connect", function() {
				console.log("connect");			
			});
	
			// Receive data from the client
			socket.on("data", function(chunk) { 
				data += chunk; 
			});
		
			// Client finished writing
			socket.on("drain", function() {
				console.log("drain");			
			});
	
			// Client finished
			socket.on("end", function() {
				try {
					var payload = JSON.parse(data);
				
					payload.command && self[payload.command] && self[payload.command](function(error, message) {
						message = message || {};
						
						socket.end(JSON.stringify(message));
					});				
				}
				catch (ex) {
					socket.end(JSON.stringify(ex));
				}

			});
	
			socket.on("close", function() {
				console.log("close");
			});
		}).listen(1997);
	
		self._server.on("error", function(error) {
			console.log(error.message);
			console.log(error.stack);
			callback && callback(error);
		});
	
		callback && callback();
	});
}


/**
 * Returns Leeloo's status information.
 *
 * @param {Function} callback `callback(error, status)`
 */
Leeloo.prototype.status = function(callback) {
	
}


/**
 * Stops Leeloo. Temporarily.
 *
 * @param {Function} callback `callback(error)` [optional]
 */
Leeloo.prototype.stopServer = function(callback) {
	var self = this;
	
	self._store.close();
	self._server.close();
	
	callback && callback(null, "bye!");
}

module.exports = new Leeloo;