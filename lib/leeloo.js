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
	this._heartbeats = 0;
	this.MAXIMUM_HEARTBEATS_PROCESSING = 3;
	
	events.EventEmitter.call(this);
}

util.inherits(Leeloo, events.EventEmitter);


/**
 * Broadcasts a payload to a listening client.
 *
 * @param {String} owner The job owner
 * @param {Object} payload JSON with keys `command` and `params`
 * @api private
 */
Leeloo.prototype._broadcast = function(owner, payload) {
	this._sockets[owner] 
			&& this._sockets[owner].writable 
			&& this._sockets[owner].write(JSON.stringify(payload) + "\n\n");
}


/**
 * Cancels a job.
 *
 * @param {Object} params
 * @param {String} params.reason The reason the job is being canceled
 * @param {ObjectID} params.id The identifier of the job to cancel [optional]
 * @param {Function} callback `callback(error)` [optional]
 */
Leeloo.prototype.cancelJob = function(params, callback) {
	var self = this;
	
	if (params instanceof Function) {
		callback = params;
		params = {};
	}
	
	// Cancel the current job if no id is supplied
	var id = params.id || self._processing._id,
		reason = params.reason || "no reason";
		
	try {
		// convert a String identifier to ObjectID
		id = (typeof id === "string" && new self._store.bson_serializer.ObjectID(params.id)) || id;
	}	
	catch (ex) {
		id = null;
		self.emit("error", ex);
	}
		
	if (id) {
		self._store.collection(self._context.mongodb.collection, function(error, collection) {
			collection.findAndModify({"_id": id}, [["_id", "asc"]], {"$set": {"s": "c", "z": reason}}, {"safe": true, "upsert": false}, function(error, upd) {
				if (upd && upd._id) {
					
					if (self._processing && self._processing._id && upd._id.equals(self._processing._id)) {
						self._idle();
					}
					
					upd.o && self._broadcast(upd.o, {"command": "canceledJob", "params": upd});
					console.log("[canceled] job " + upd._id.toString() + " `" + reason + "`");
				}
				else if (self._processing && self._processing._id && self._processing._id.equals(id)) {
					// For some reason we can't find the record for the currently processing job
					self._idle();
				};
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
	var self = this,
		data = "",
		callback = (host instanceof Function)
			? host
			: callback,
		host = (host instanceof Function)
			? "127.0.0.1"
			: host || "127.0.0.1";
	
	self._connection = net.createConnection(port, host);
	self._connection.removeAllListeners();	
	self._connection.setEncoding("utf8");
	self._connection.setKeepAlive(true);
	
	self._connection.on("connect", function() {
		self.emit("connected");
		callback && callback();
	});
	
	self._connection.on("data", function(chunk) {
		try {
			data += chunk;
			var payloads = data.split("\n\n");
			data = "";
			
			if (payloads.length) {
				self._connection.pause();
				
				payloads.forEach(function(payload) {
					if (payload.slice(-1) === "}") {
						payload = JSON.parse(payload);
						payload.command && self.emit(payload.command, payload.params || {});
					}
					else if (payload.length) {
						data = payload;
						self._connection.resume();
					}
					else {
						self._connection.resume();
					}
				});				
			}
		}
		catch (ex) {
			self.emit("error", ex);
		}		
	});
		
	self._connection.on("end", function() {
		self.emit("disconnected");
	});
	
	
	self._connection.on("drain", function() {
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
		},
		callback = (params instanceof Function)
			? params
			: callback,
		params = (params instanceof Function)
			? {}
			: params;
			
	payload.params = params || {};
	
	this._connection.writable && this._connection.write(JSON.stringify(payload) + "\n\n");
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
		self._idle();
		
		self._store.collection(self._context.mongodb.collection, function(error, collection) {
			collection.findAndModify({"_id": new self._store.bson_serializer.ObjectID(params.id), "s": "p"}, [["id", "asc"]], {"$set": {"s": "f"}}, {"safe": true, "upsert": false}, function(error, upd) {
				if (upd) {
					console.log("[finished] job " + ((upd && upd._id && upd._id.toString()) || ""));
					upd.o && self._broadcast(upd.o, {"command": "finishedJob", "params": upd || {}});
					
					if (upd && upd.r) {
						// Reschdule this job for the fuuuuuture
						self.schedule({"when": Date.now() + upd.r, "reschedule": upd.r, "xid": upd.xid, "ext": upd.x || {}, "type": upd.t});
					}
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
			console.log("\tcancel-job")
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
					if (self._heartbeats >= self.MAXIMUM_HEARTBEATS_PROCESSING) {
						console.log("[heartbeat][" + Object.keys(self._sockets).length + "] processing " + ((self._processing && self._processing._id) || "") + " (" + self._heartbeats + ")");			
						if (self._processing && self._processing._id) {
							self.cancelJob({"reason": "timed out", "id": self._processing._d});	
						} 
						else {
							self._idle();
						}
					}
					else {
						console.log("[heartbeat][" + Object.keys(self._sockets).length + "] processing " + ((self._processing && self._processing._id) || "") + " (" + self._heartbeats + ")");			
						self._heartbeats += 1;
					}
					break;
					
				default:
					console.log("[heartbeat][" + Object.keys(self._sockets).length + "] " + self._status);
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
					self._heartbeats = 0;
					job_in_progress.o && self._broadcast(job_in_progress.o, {"command": "processJob", "params": job_in_progress});
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


Leeloo.prototype._idle = function() {
	this._status = "idle";
	this._heartbeats = 0;
	this._processing = {};					
	this.processJobs();
	return 1;
}


/**
 * Opens the MongoDB database.
 *
 * @param {Function} callback `callback(error)`
 */
Leeloo.prototype._openDatabase = function(callback) {
	if (this._context.mongodb && this._context.mongodb.using_replica_sets) {
		var servers = [];
		
		this._context.mongodb.servers.forEach(function(server) {
			servers.push(new Server(server.host, server.port, {"auto_reconnect": true}));
		});
		
		// Connect to our databases and open
		this._store = new Db(this._context.mongodb.datastore, new ReplSetServers(servers, {"rs_name": this._context.mongodb.replica_set_name}));
	}
	else {
		// Connect to our database
		this._store = new Db(this._context.mongodb.datastore, new Server(this._context.mongodb.servers[0].host, this._context.mongodb.servers[0].port, {"auto_reconnect": true}));
	}

	this._store.open(callback);	
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
		// Grab the highest priority and oldest job that's been scheduled before now
		collection.findOne({"s": "s", "w": {"$lt": Date.now()}}, {"sort": {"p": -1, "w": 1}}, function(error, job) {
			if (error) {
				self.emit("error", error);
			}
			else if (job && self._status === "idle") {
				collection.findAndModify({"_id": job._id}, [["_id", "asc"]], {"$set": {"s": "p"}}, {"safe": true}, function(error, upd) {
					console.log("[processing] job " + job._id.toString());
					self._status = "processing";
					self._processing = upd;
					upd.o && self._broadcast(upd.o, {"command": "processJob", "params": upd});
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
 * @param {String} params.type What type of job is this? [optional]
 * @param {Boolean} params.reschedule How long after completion (ms) should we reschedule? [optional]
 * @param {Object} params.ext Additional external parameters
 * @param {Function} callback `callback(error)` [optional]
 */
Leeloo.prototype.schedule = function(params, callback) {
	var self = this,
		callback = (params instanceof Function)
			? params
			: callback,
		params = (params instanceof Function)
			? {}
			: params || {},
		job = {};
	
	// When should the job run
	job.w = params.when || (60000 + Date.now());
	
	// Type of job
	job.t = params.type || "u";
	
	// Reschedule on finish if > 0
	job.r = params.reschedule || 0;
	
	// External id
	job.xid = params.xid;
	
	// Additional external parameters
	job.x = params.ext || {};
	
	// Current job status
	job.s = "s";
	
	// Current job priority
	job.p = params.priority || 0;
	
	// Job owner
	job.o = params.o || "anonymous";
	
	if (job.xid) {
		self._store.collection(self._context.mongodb.collection, function(error, collection) {
			collection.find({"xid": job.xid, "t": job.t, "s": "s"}, {"limit": 1}, function(error, cursor) {
				if (error) {
					console.log(error);
				}
				else {
					cursor.toArray(function(error, prev) {
						if (error) {
							console.log(error);
						}
						else if (prev.length) {
							if (job.w > prev[0].w) {
								collection.findAndModify(
										{"_id": prev[0]._id},
										[["_id", "asc"]],
										{"$set": {"w": job.w}}, 
										{"safe": true, "multi": false, "upsert": false, "new": true},
										function(error, upd) {
									callback && callback(error, {"command": "scheduledJob", "params": upd});
								});
							}
							else {
								callback && callback(error, {"command": "scheduledJob", "params": (prev && prev[0]) || {}});
							}
						}
						else {							
							collection.insert(job, {"safe": true}, function(error, documents) {	
								callback && callback(error, {"command": "scheduledJob", "params": (documents && documents[0]) || {}});
							});
						}
					});
				}
			});
		});
	}
	else {
		callback && callback(new Error("`xid` is a required parameter"));
	}
}


/**
 * Schedules many jobs.
 *
 * @param {Array} jobs An array of jobs
 * @param {Function} callback `callback(error)`
 */
Leeloo.prototype.scheduleJobs = function(owner, jobs, callback) {
	var self = this,
		j = (util.isArray(jobs) && jobs && jobs.length) || 0,
		scheduled = [],
		scheduleJob = function(i) {
			if (i > j - 1) {
				self._broadcast(owner, {"command": "scheduledJobs", "params": scheduled});
			}
			else {
				jobs[i].o = owner;
				
				self.schedule(jobs[i], function(error, job) {
					(error && scheduled.push({"error": error.message})) || scheduled.push(job);
					scheduleJob(i + 1);
				});
			}
		};
		
	if (j) {
		scheduleJob(0);
	}
	else {
		callback && callback(null, scheduled);
	}
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
			var data = "";
						
			socket.setEncoding("utf8");
			socket.setKeepAlive(true);
	
			// A client is connecting
			socket.on("connect", function() {
				console.log("[" + socket.remoteAddress + "] connected; waiting for identification.");
			});
	
			// Receive data from the client
			socket.on("data", function(chunk) {
				try {
					data += chunk;
					payloads = data.split("\n\n");
					data = "";
					
					if (payloads.length) {
						socket.pause();
						
						payloads.forEach(function(payload) {
							if (payload.slice(-1) === "}") {
								payload = JSON.parse(payload);
								
								if (payload.command === "identifyClient" && payload.params && payload.params.identity) {
									self._sockets[payload.params.identity] = socket;
									socket.identity = payload.params.identity;
								}
								
								if (!util.isArray(payload.params)) {
									payload.params.o = socket.identity;
								}

								console.log("[" + socket.remoteAddress + "]" + ((socket.identity && " [" + socket.identity + "] ") || " ") + payload.command + " " + JSON.stringify(payload.params));
								
								if (payload.command !== "scheduleJobs") {
									payload.command && self[payload.command] && self[payload.command](payload.params, function(error, message) {
										message = message || {};

										socket.write(JSON.stringify(message) + "\n\n");
									});	
								}
								else {
									self.scheduleJobs(socket.identity, payload.params, function(error, message) {
										message = message || {};

										socket.write(JSON.stringify(message) + "\n\n");
									});	
								}
							}
							else if (payload.length) {
								data = payload;
								socket.resume();
							}
							else {
								socket.resume();
							}
						});
					}
				}
				catch (ex) {
					// incomplete?
					socket.write(JSON.stringify({"error": "failed to parse JSON (send not finished?)"}));
				}
			});
		
			// Client finished writing
			socket.on("drain", function() {
			});
	
			// Client finished
			socket.on("end", function() {
				console.log("[" + socket.remoteAddress + "] disconnected");		
				socket.removeAllListeners();
				if (socket.identity) {
					delete self._sockets[socket.identity];
				}
				console.log(self._sockets);
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