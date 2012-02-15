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
	this._context = require("../conf/env.json");
	this._sockets = {};
	this._started;
	this._status = "idle";	
	
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
 * Server heartbeat. Check for jobs. Print current status.
 */
Leeloo.prototype._heartbeat = function() {
	var self = this;
	
	console.log("[heartbeat] " + self._status);
	
	setTimeout(function() { self._heartbeat(); }, self._context && self._context.heartbeat || 60000);
	
	if (self._status === "idle") {
		self.processJobs();
	}
}


/**
 * Opens the MongoDB database.
 *
 * @param {Function} callback `callback(error)`
 */
Leeloo.prototype._openDatabase = function(callback) {
	var self = this;
		
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
Leeloo.prototype.list = function(params, callback) {
	var self = this;
	
	self._store.collection(self._context.mongodb.collection, function(error, collection) {
		collection.find({"s": "s"}, {}, {}, function(error, cursor) {
			cursor.toArray(function(error, array) {
				callback && callback(error, {"jobs": array});
			});
		});
	});
}


/**
 * Processes jobs in the queue.
 *
 * @param {Function} callback `callback(error)`
 */
Leeloo.prototype.processJobs = function() {
	
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
	job.w = (params.when || params.w || 60000) + Date.now();
	job.t = params.type || "u";
	job.r = params.reschedule || 0;
	job.s = "s";
	
	self._store.collection(self._context.mongodb.collection, function(error, collection) {
		collection.insert(job, {"safe": true}, function(error, documents) {
			callback && callback();
		});
	});
}


/**
 * Starts Leeloo.
 *
 * @param {Function} callback `callback(error)` [optional]
 */
Leeloo.prototype.startServer = function(params, callback) {
	var self = this;
	
	self._openDatabase(function(error) {
		self._started = Date.now();
		
		self._heartbeat();
		
		self._server = net.createServer({"allowHalfOpen": true}, function(socket) {
			var id = "" + (Math.floor(Math.random() * 1000000) + Date.now());
			
			self._sockets[id] = socket;
			
			socket.setEncoding("utf8");
	
			// A client is connecting
			socket.on("connect", function() {
				console.log("[" + socket.remoteAddress + "] connected");
			});
	
			// Receive data from the client
			socket.on("data", function(chunk) { 			
				try {
					var payload = JSON.parse(chunk);
					
					console.log("[" + socket.remoteAddress + "] " + payload.command + " " + JSON.stringify(payload.params));
				
					payload.command && self[payload.command] && self[payload.command](payload.params, function(error, message) {
						message = message || {};
						
						socket.write(JSON.stringify(message));
					});				
				}
				catch (ex) {
					socket.write(JSON.stringify(ex));
				}
			});
		
			// Client finished writing
			socket.on("drain", function() {	
			});
	
			// Client finished
			socket.on("end", function() {
				console.log("[" + socket.remoteAddress + "] disconnected");		
				socket.removeAllListeners();
				delete self._sockets[id];
				socket.end();
			});
	
			socket.on("close", function() {
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
Leeloo.prototype.status = function(params, callback) {
	callback(null, {"status": this._status, "uptime": Date.now() - this._started})
}


/**
 * Stops Leeloo. Temporarily.
 *
 * @param {Function} callback `callback(error)` [optional]
 */
Leeloo.prototype.stopServer = function(params, callback) {
	var self = this;
	
	self._store.close();
	self._server.close();
	
	callback && callback(null, "bye!");
	process.exit();
}

module.exports = new Leeloo;