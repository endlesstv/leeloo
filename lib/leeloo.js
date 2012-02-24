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
	// Client stuff
	this._connection;
	
	// Server stuff
	this._context = require("../conf/env.json");
	this._sockets = {};
	this._started;
	this._status = "idle";	
	this._processing;
	this.MAXIMUM_HEARTBEATS_PROCESSING = 3;
	
	events.EventEmitter.call(this);
}

util.inherits(Leeloo, events.EventEmitter);


/**
 * Cancels a job.
 *
 * @param {String} reason The reason the job is being canceled
 * @param {ObjectID} id The identifier of the job to cancel [optional]
 * @param {Function} callback `callback(error)` [optional]
 */
Leeloo.prototype.cancelJob = function(reason, id, callback) {
	var self = this;
	
	if (id instanceof Function) {
		callback = id;
		id = null;
	}
	
	id = id || self._processing._id;
	
	if (id) {
		self._store.collection(self._context.mongodb.collection, function(error, collection) {
			collection.findAndModify({"_id": id}, [["_id", "asc"]], {"$set": {"s": "c", "z": reason}}, {"safe": true, "upsert": false}, function(error, upd) {
				console.log("[canceled] job " + upd._id.toString() + " `" + reason + "`");
				self._status = "idle";
				self._processing = {};
				self._processing.heartbeats = 0;
				
				Object.keys(self._sockets).forEach(function(key) {
					self._sockets[key].write(JSON.stringify(upd));
				});
			});
		});		
	}
	else {
		callback && callback();
	}
}


/**
 * Connects to Leeloo.
 *
 * @param {Integer} port The port Leeloo is running on
 * @param {String} host The host running Leeloo [optional]
 * @param {Function} callback `callback(error)` [optional]
 */
Leeloo.prototype.connect = function(port, host, callback) {
	var self = this;
	
	if (host instanceof Function) {
		callback = host;
		host = "127.0.0.1";
	}
	
	self._connection = net.createConnection(port, host);
	self._connection.removeAllListeners();	
	self._connection.setEncoding("utf8");
	
	self._connection.on("connect", function() { 
		callback && callback();
	});
	
	self._connection.on("data", function(response) {
		try {
			var data = JSON.parse(response);
			
			data.command && self.emit(data.command, data.params);
		}
		catch (ex) {
			self.emit("error", ex);
		}		
	});
	
	self._connection.on("error", function(error) {
		self.emit("error", error);
	});
}


/**
 * Ends the current connection to Leeloo.
 *
 * @param {Function} callback `callback(error)` [optional]
 */
Leeloo.prototype.disconnect = function(callback) {
	var self = this;
	
	self._connection && self._connection.end();
	
	callback && callback();
}


/**
 * Commands Leeloo to do something.
 *
 * @param {String} command The command to issue
 * @param {String} params Additional parameters [optional]
 * @param {Function} callback `callback(error)` [optional]
 */
Leeloo.prototype.dispatch = function(command, params, callback) {
	var self = this,
		payload = {
			"command": command
		};
		
	if (params instanceof Function) {
		callback = params;
		params = null;
	}
	
	payload.params = params || {};
	
	self._connection.write(JSON.stringify(payload));
}


/**
 * Finishes a job in process.
 *
 * @param {Object} params
 * @param {ObjectID} params.id The job to finish
 * @param {Function} callback `callback(error)` [optional]
 */
Leeloo.prototype.finishJob = function(params, callback) {
	var self = this;
	
	if (params.id) {
		self._store.collection(self._context.mongodb.collection, function(error, collection) {
			collection.findAndModify({"_id": params.id, "s": "p"}, [["id", "asc"]], {"$set": {"s": "f"}}, {"safe": true, "upsert": false}, function(error, upd) {
				if (upd) {
					console.log("[finished] job " + upd._id.toString());
					self._status = "idle";
					self._processing = {};
					self._processing.heartbeats = {};
					
					Object.keys(self._sockets).forEach(function(key) {
						self._sockets[key].write(JSON.stringify({"command": "finished", "params": upd}));
					});
				}
			});
		});
	}
	else {
		callback && callback();
	}
}


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
 *
 * @param {Boolean} check Should we check for existing jobs on this heartbeat
 */
Leeloo.prototype._heartbeat = function(check) {
	var self = this,
		printHeartbeat = function() {
			switch (self._status) {
				case "processing":
					if (self._processing.heartbeats >= self.MAXIMUM_HEARTBEATS_PROCESSING) {
						console.log("[heartbeat] processing " + self._processing._id + " (" + self._processing.heartbeats + ")");			
						self.cancelJob("timed out");
					}
					else {
						console.log("[heartbeat] processing " + self._processing._id + " (" + self._processing.heartbeats + ")");			
						self._processing.heartbeats += 1;
					}
					break;
				default:
					console.log("[heartbeat] " + self._status);
					self.processJobs();		
			}			
		};
	
	if (check) {
		// We are starting the server and don't know what our status is
		self._store.collection(self._context.mongodb.collection, function(error, collection) {
			collection.findOne({"s": "p"}, function(error, job_in_progress) {
				if (job_in_progress) {
					self._status = "processing";
					self._processing = job_in_progress;
					self._processing.heartbeats = 0;
					
					Object.keys(self._sockets).forEach(function(key) {
						self._sockets[key].write(JSON.stringify({"command": "processJob", "params": job}));
					});
				}

				printHeartbeat();
			});
		});
	}
	else {		
		printHeartbeat();
	}
	
	setTimeout(function() { self._heartbeat(); }, self._context && self._context.heartbeat || 60000);
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
	var self = this;
	
	self._store.collection(self._context.mongodb.collection, function(error, collection) {
		// Grab the first scheduled job
		collection.findOne({"s": "s", "w": {"$lt": Date.now()}}, {"sort": [["w", "asc"]]}, function(error, job) {
			if (job) {
				collection.findAndModify({"_id": job._id}, [["_id", "asc"]], {"$set": {"s": "p"}}, {"safe": true}, function(error, upd) {
					console.log("[processing] job " + job._id.toString());
					self._status = "processing";
					self._processing = upd;
					self._processing.heartbeats = 0;
					
					Object.keys(self._sockets).forEach(function(key) {
						self._sockets[key].write(JSON.stringify({"command": "processJob", "params": job}));
					});
				});
			}
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
	job.w = (params.when || params.w || 60000) + Date.now();
	job.t = params.type || "u";
	job.r = params.reschedule || 0;
	job.s = "s";
	
	self._store.collection(self._context.mongodb.collection, function(error, collection) {
		collection.insert(job, {"safe": true}, function(error, documents) {
			callback && callback(error, {"job": documents[0] && documents[0]._id});
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
		
		self._heartbeat(true);
		
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
					
					payload.params = payload.params || {};
					payload.params.o = id;
					
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