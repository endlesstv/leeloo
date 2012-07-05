/**
 * "Multi-pass!"
 * "Yes, dear, she knows it's a multipass."
 */
var events = require("events"),
	net = require("net"),
	util = require("util"),
	Db = require("mongodb").Db,
	Server = require("mongodb").Server,
	ReplSet = require("mongodb").ReplSet;
	
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
	this.command_queue = [];
	this.command_idle = true;
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
Leeloo.prototype.cancelJob = function(owner, params, callback) {
	var self = this,
		callback = (params instanceof Function)
			? params
			: callback,
		params = (params instanceof Function)
			? {}
			: params || {};
			
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
					
					console.log("[canceled] job " + upd._id.toString() + " `" + reason + "`");
					self._broadcast(owner, {"command": "canceledJob", "params": upd});
				}
				else if (self._processing && self._processing._id && self._processing._id.equals(id)) {
					// For some reason we can't find the record for the currently processing job
					self._idle();
				}
				
				return callback && callback();
			});
		});		
	}
	else {
		return callback && callback();
	}
}


/**
 * Finishes a job in process.
 *
 * @param {Object} params
 * @param {ObjectID} params.id The job to finish
 * @param {Function} callback `callback(error)` [optional]
 */
Leeloo.prototype.finishJob = function(owner, params, callback) {
	var self = this;
	
	if (params.id) {
		self._idle();
		
		self._store.collection(self._context.mongodb.collection, function(error, collection) {
			if (error) {
				console.log(util.inspect(error, true, 7, true));
				return callback && callback();
			}
			
			collection.findAndModify({"_id": new self._store.bson_serializer.ObjectID(params.id), "s": "p"}, [["id", "asc"]], {"$set": {"s": "f"}}, {"safe": true, "upsert": false}, function(error, upd) {
				if (error) {
					console.log(util.inspect(error, true, 7, true));
					return callback && callback();
				}
								
				if (upd) {
					console.log("[finished] job " + ((upd && upd._id && upd._id.toString()) || ""));
					upd.o && self._broadcast(owner, {"command": "finishedJob", "params": upd || {}});
					
					if (upd && upd.r) {
						// Reschdule this job for the fuuuuuture
						self.schedule(owner, {"when": Date.now() + upd.r, "reschedule": upd.r, "xid": upd.xid, "ext": upd.x || {}, "type": upd.t});
					}
				}
				
				return callback && callback();
			});
		});
	}
	else {
		return callback && callback();
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
			var num_conn = Object.keys(self._sockets).length;
			switch (self._status) {
				case "processing":
					if (self._heartbeats >= self.MAXIMUM_HEARTBEATS_PROCESSING) {
						console.log("[heartbeat][" + num_conn + "][" + ((self._processing && self._processing.o) || "anon") + "] processing " + ((self._processing && self._processing._id) || "") + " (" + self._heartbeats + ")");			
						if (self._processing && self._processing._id) {
							self.cancelJob(self._processing.o, {"reason": "timed out", "id": self._processing._d});	
						} 
						else {
							self._idle();
						}
					}
					else {
						console.log("[heartbeat][" + num_conn + "][" + ((self._processing && self._processing.o) || "anon") + "] processing " + ((self._processing && self._processing._id) || "") + " (" + self._heartbeats + ")");			
						self._heartbeats += 1;
					}
					break;
					
				default:
					// Don't process jobs if there are no consumers connected
					console.log("[heartbeat][" + num_conn + "] " + self._status);
					num_conn && self.processJobs();		
			}			
		};
	
	if (check) {
		// We are starting the server and don't know what our status is
		self._store.collection(self._context.mongodb.collection, function(error, collection) {
			if (error) {
				console.log(util.inspect(error, true, 7, true));
				return;
			}
						
			collection.findOne({"s": "p"}, function(error, job_in_progress) {
				if (error) {
					console.log(util.inspect(error, true, 7, true));
					return;
				}
								
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
	
	if (self.command_idle && self.command_queue.length) {
		self.emit("processCommand");
	}
	
	setTimeout(function() { self._heartbeat(); }, (self._context && self._context.heartbeat) || 60000);
}


Leeloo.prototype._idle = function() {
	this._status = "idle";
	this._heartbeats = 0;
	this._processing = {};					
	this.processJobs();
	return 1;
}


Leeloo.prototype.idleCommand = function() {
	this.command_idle = true;
	this.emit("processCommand");
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
			servers.push(new Server(server.host, server.port, {"auto_reconnect": true, "poolSize": 5, "readPreference": Server.READ_SECONDARY}));
		});
		
		// Connect to our databases and open
		this._store = new Db(this._context.mongodb.datastore, new ReplSet(servers, {"readPreference": Server.READ_SECONDARY, "rs_name": this._context.mongodb.replica_set_name, "socketOptions": {"keepAlive": 1, "timeout": 1000}}));
	}
	else {
		// Connect to our database
		this._store = new Db(this._context.mongodb.datastore, new Server(this._context.mongodb.servers[0].host, this._context.mongodb.servers[0].port, {"auto_reconnect": true}));
	}

	this._store.open(callback);	
}


/**
 * Processes jobs in the queue.
 *
 * @param {Function} callback `callback(error)`
 */
Leeloo.prototype.processJobs = function() {
	var self = this;
	
	self._store.collection(self._context.mongodb.collection, function(error, collection) {
		if (error) {
			console.log("error generating collection")
			console.log(util.inspect(error, true, 7, true));
			return;
		}
				
		// Grab the highest priority and oldest job that's been scheduled before now
		collection.findOne({"s": "s", "w": {"$lt": Date.now()}}, {}, {"sort": {"p": -1, "w": 1}}, function(error, job) {
			if (error) {
				console.log(util.inspect(error, true, 7, true));
				self.emit("error", error);
				return;
			}
			
			if (job && self._status === "idle") {
				collection.findAndModify({"_id": job._id}, [["_id", "asc"]], {"$set": {"s": "p"}}, {"safe": true}, function(error, upd) {
					if (error) {
						console.log(util.inspect(error, true, 7, true));
						return;
					}
										
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
Leeloo.prototype.schedule = function(owner, params, callback) {
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
					console.log(util.inspect(error, true, 7, true));
					return callback && callback();
				}
				
				cursor.toArray(function(error, prev) {
					if (error) {
						console.log(util.inspect(error, true, 7, true));
						return callback && callback();
					}
					
					if (prev.length) {
						if (job.w > prev[0].w) {
							collection.findAndModify(
									{"_id": prev[0]._id},
									[["_id", "asc"]],
									{"$set": {"w": job.w}}, 
									{"safe": true, "multi": false, "upsert": false, "new": true},
									function(error, upd) {
								self._broadcast(owner, {"command": "scheduledJob", "params": upd || {}});
								callback && callback();
							});
						}
						else {
							self._broadcast(owner, {"command": "scheduledJob", "params": prev[0]});								
							return callback && callback();								
						}
					}
					else {							
						collection.insert(job, {"safe": true}, function(error, documents) {	
							if (error) {
								console.log(util.inspect(error, true, 7, true));
								return callback && callback();
							}
														
							self._broadcast(owner, {"command": "scheduledJob", "params": (documents && documents[0]) || {}});
							return callback && callback();
						});
					}
				});
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
				
				self.schedule(owner, jobs[i], function(error, job) {					
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
	var self = this,
		callback = (params instanceof Function)
			? params
			: callback,
		params = (params instanceof Function)
			? {}
			: params || {};
	
	self._openDatabase(function(error) {
		if (error) {
			console.log("unable to open database")
			console.log(util.inspect(error, true, 7, true));
			return callback && callback();
		}
		else {
			console.log("connected to the database")
		}
				
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
								
								if (payload.command !== "identifyClient") {
									self.command_queue.push({
										"command": payload.command,
										"owner": socket.identity,
										"params": payload.params,
									});
									self.command_idle && self.emit("processCommand");
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
			
			socket.on("close", function(had_error) {
				console.log("[" + socket.identity + "] disconnected; closed" + (had_error ? " with error" : ""));
			});
	
			// Client finished
			socket.on("end", function() {
				console.log("[" + socket.identity + "] disconnected");		
				socket.removeAllListeners();
				if (socket.identity) {
					delete self._sockets[socket.identity];
				}
				socket.end();
			});
		}).listen(1997);
	
		self._server.on("error", function(error) {
			if (error) {
				console.log(error.message);
				console.log(error.stack);				
				console.log(util.inspect(error, true, 7, true));
				return;
			}			
		});
		
		
		// Process commands in order as they roll in
		self.on("processCommand", function() {
			if (self.command_queue.length) {
				self.command_idle = false;
				
				var command = self.command_queue.shift();
				
				console.log("command " + command.command + " issued (" + self.command_queue.length + ")");
				self[command.command](command.owner, command.params, function() {
					console.log("command " + command.command + " finished (" + self.command_queue.length + ")");
					self.emit("processCommand");
				});
			}
			else {
				self.command_idle = true;
			}
		});
	
		callback && callback();
	});
}



/**
 * Stops Leeloo. Temporarily.
 *
 * @param {Function} callback `callback(error)` [optional]
 */
Leeloo.prototype.stopServer = function(params, callback) {
	this._store.close();
	this._server.close();
	
	callback && callback(null, "bye!");
	process.exit();
}

module.exports = new Leeloo;