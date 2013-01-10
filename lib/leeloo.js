/**
 * "Multi-pass!"
 * "Yes, dear, she knows it's a multipass."
 */
var events = require("events");
var util = require("util");
var ObjectID = require("mongodb").ObjectID;
	
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
	this.heartbeat_interval = null;
	this.reap_interval = null;
	
	events.EventEmitter.call(this);
};

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
};

/**
 * Cancels a job.
 *
 * @param {Object} params
 * @param {String} params.reason The reason the job is being canceled
 * @param {ObjectID} params.id The identifier of the job to cancel [optional]
 * @param {Function} callback `callback(error)` [optional]
 */
Leeloo.prototype.cancelJob = function(owner, params, callback) {
	var callback = (params instanceof Function) ? params : callback;
	var params = (params instanceof Function) ? {} : params || {};
			
	// Cancel the current job if no id is supplied
	var id = params.id || this._processing._id;
	var reason = params.reason || "no reason";
		
	try {
		// convert a String identifier to ObjectID
		id = typeof id === "string" ? new ObjectID(params.id) : id;
	}	
	catch (ex) {
		id = null;
		this.emit("error", ex);
		return callback && callback();
	};
		
	this._store.collection(this._context.mongodb.collection, (function(error, collection) {
		collection.findAndModify({"_id": id}, [["_id", "asc"]], {"$set": {"s": "c", "z": reason}}, {"safe": true, "upsert": false}, (function(error, upd) {
			if (upd && upd._id) {
				
				if (this._processing && this._processing._id && upd._id.equals(this._processing._id)) {
					this._idle();
				};

				console.log("[canceled] job " + upd._id.toString() + " `" + reason + "`");
				this._broadcast(owner, {"command": "canceledJob", "params": upd});
			}
			else if (this._processing && this._processing._id && this._processing._id.equals(id)) {
				// For some reason we can't find the record for the currently processing job
				this._idle();
			};
			
			return callback && callback();
		}).bind(this));
	}).bind(this));
};


/**
 * Finishes a job in process.
 *
 * @param {Object} params
 * @param {ObjectID} params.id The job to finish
 * @param {Function} callback `callback(error)` [optional]
 */
Leeloo.prototype.finishJob = function(owner, params, callback) {
	if (!params || !params.id) {
		return callback && callback();
	}
	
	this._idle();
	
	this._store.collection(this._context.mongodb.collection, (function(error, collection) {
		if (error) {
			console.log(util.inspect(error, true, 7, true));
			return callback && callback();
		};
		
		collection.findAndModify({"_id": new ObjectID(params.id), "s": "p"}, [["id", "asc"]], {"$set": {"s": "f"}}, {"safe": true, "upsert": false}, (function(error, upd) {
			if (error) {
				console.log(util.inspect(error, true, 7, true));
				return callback && callback();
			};

			if (upd) {
				console.log("[finished] job " + ((upd && upd._id && upd._id.toString()) || ""));
				upd.o && this._broadcast(owner, {"command": "finishedJob", "params": upd || {}});
				
				if (upd && upd.r) {
					// Reschdule this job for the fuuuuuture
					this.schedule(owner, {"when": Date.now() + upd.r, "reschedule": upd.r, "xid": upd.xid, "ext": upd.x || {}, "type": upd.t});
				};
			};
			
			return callback && callback();
		}).bind(this));
	}).bind(this));
};

Leeloo.prototype.handleError = function(error, location) {
	if (error) {
		location && console.log(location);
		error.stack && console.log(error.stack);
		console.log(util.inspect(error, true, 7, true));
		return;
	};
};

Leeloo.prototype.handleSocket = function(socket) {
	var data = "";
				
	socket.setEncoding("utf8");
	socket.setKeepAlive(true);

	// A client is connecting
	socket.on("connect", (function() {
		console.log("[" + socket.remoteAddress + "] connected; waiting for identification.");
	}).bind(this));

	// Receive data from the client
	socket.on("data", (function(chunk) {
		try {
			data += chunk;
			var payloads = data.split("\n\n");
			data = "";
			
			if (payloads.length) {
				socket.pause();
				
				payloads.forEach((function(payload) {
					if (payload.slice(-1) === "}") {
						payload = JSON.parse(payload);
						
						if (payload.command === "identifyClient" && payload.params && payload.params.identity) {
							this._sockets[payload.params.identity] = socket;
							socket.identity = payload.params.identity;
						}
						
						if (!util.isArray(payload.params)) {
							payload.params.o = socket.identity;
						}

						console.log("[" + socket.remoteAddress + "]" + ((socket.identity && " [" + socket.identity + "] ") || " ") + payload.command + " " + JSON.stringify(payload.params));
						
						if (payload.command !== "identifyClient") {
							this.command_queue.push({
								"command": payload.command,
								"owner": socket.identity,
								"params": payload.params,
							});
							this.command_idle && this.emit("processCommand");
						}
					}
					else if (payload.length) {
						data = payload;
						socket.resume();
					}
					else {
						socket.resume();
					};
				}).bind(this));
			};
		}
		catch (ex) {
			// incomplete?
			socket.write(JSON.stringify({"error": "failed to parse JSON (send not finished?)"}));
		}
	}).bind(this));

	// Client finished writing
	socket.on("drain", (function() {
	}).bind(this));
	
	socket.on("close", (function(had_error) {
		console.log("[" + socket.identity + "] disconnected; closed" + (had_error ? " with error" : ""));
	}).bind(this));

	// Client finished
	socket.on("end", (function() {
		console.log("[" + socket.identity + "] disconnected");		
		socket.removeAllListeners();
		if (socket.identity) {
			delete this._sockets[socket.identity];
		};
		socket.end();
	}).bind(this));
};


/**
 * Prints a help message.
 *
 * @param {String} details Receive detailed information about this command [optional]
 * @param {Function} callback `callback(error)` [optional]
 */
Leeloo.prototype.help = function(details, callback) {
	var callback = details instanceof Function ? details : callback;
	var details = details instanceof Function ? "" : details || "";
	
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
	};
	
	return callback && callback();
};

/**
 * Server heartbeat. Check for jobs. Print current status.
 *
 * @param {Boolean} check Should we check for existing jobs on this heartbeat
 */
Leeloo.prototype._heartbeat = function(check) {
	var num_conn = Object.keys(this._sockets).length;
	switch (this._status) {
		case "processing":
			if (this._heartbeats >= this.MAXIMUM_HEARTBEATS_PROCESSING) {
				console.log("[heartbeat][" + num_conn + "][" + ((this._processing && this._processing.o) || "anon") + "] processing " + ((this._processing && this._processing._id) || "") + " (" + this._heartbeats + ")");
				if (this._processing && this._processing._id) {
					this.cancelJob(this._processing.o, {"reason": "timed out", "id": this._processing._d});	
				} 
				else {
					this._idle();
				}
			}
			else {
				console.log("[heartbeat][" + num_conn + "][" + ((this._processing && this._processing.o) || "anon") + "] processing " + ((this._processing && this._processing._id) || "") + " (" + this._heartbeats + ")");
				this._heartbeats += 1;
			};
			break;
					
		default:
			// Don't process jobs if there are no consumers connected
			console.log("[heartbeat][" + num_conn + "] " + this._status);
			num_conn && this.processJobs();
	};
	
	if (this.command_idle && this.command_queue.length) {
		this.emit("processCommand");
	};
/*
	if (check) {
		check = false;
		// We are starting the server and don't know what our status is
		this._store.collection(this._context.mongodb.collection, (function(error, collection) {
			if (error) {
				this.emit("error", error, "heartbeat: open collection");
				return;
			}
						
			collection.findOne({"s": "p", "o": {"$in": }}, (function(error, job_in_progress) {
				if (error) {
					this.emit("error", error, "heartbeat: find processing job");
					return;
				}
								
				if (job_in_progress) {
					this._status = "processing";
					this._processing = job_in_progress;
					this._heartbeats = 0;
					job_in_progress.o && this._broadcast(job_in_progress.o, {"command": "processJob", "params": job_in_progress});
				}

				printHeartbeat();
			}).bind(this));
		}).bind(this));
	}
*/
};


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
	var Db = require("mongodb").Db;
	var Server = require("mongodb").Server;
	var ReplSet = require("mongodb").ReplSet;
	var ReadPreference = require("mongodb").ReadPreference;
	
	if (this._context.mongodb && this._context.mongodb.using_replica_sets) {
		var servers = [];

		// Set up the MongoDB replica set servers
		this._context.mongodb.servers.forEach(function(server) {
			var server_info = new Server(server.host, server.port, {
					"auto_reconnect": true, 
					"poolSize": 5
				});

			servers.push(server_info);
		});

		var replSet = new ReplSet(servers, {"rs_name": this._context.mongodb.replica_set_name, "socketOptions": {"keepAlive": 1}});

		// Connect to our replica set and open
		var db_pref = {
			"slaveOk": true,
			"readPreference": ReadPreference.SECONDARY_PREFERRED
		};
		
		// connect to our replica set
		this._store = new Db(this._context.mongodb.datastore, replSet, db_pref);
	}
	else {
		// connect to our datastore
		this._store = new Db(this._context.mongodb.datastore, new Server(this._context.mongodb.servers[0].host, this._context.mongodb.servers[0].port, {"auto_reconnect": true}));
	}

	// open and return
	return this._store.open(callback);	
};

Leeloo.prototype.processCommand = function() {
	if (!this.command_queue.length) {
		// there are no commands left to process
		this.command_idle = true;
		return;
	};
	this.command_idle = false;
	var command = this.command_queue.shift();
	console.log("command " + command.command + " issued (" + this.command_queue.length + ")");
	this[command.command].call(this, command.owner, command.params, (function() {
		console.log("command " + command.command + " finished (" + this.command_queue.length + ")");
		return this.emit("processCommand");
	}).bind(this));
};

/**
 * Processes jobs in the queue.
 *
 * @param {Function} callback `callback(error)`
 */
Leeloo.prototype.processJobs = function() {
	var owners = Object.keys(this._sockets);
	
	this._store.collection(this._context.mongodb.collection, (function(error, collection) {
		if (error) {
			return this.emit("error", error, "processJobs: open collection");
		};
		
		if (!owners) {
			// don't process jobs if we have no clients connected
			return;
		};
		
		// Grab the highest priority and oldest job that's been scheduled before now
		collection.findOne({"s": "s", "w": {"$lt": Date.now()}, "o": {"$in": owners}}, {}, {"sort": {"p": -1, "w": 1}}, (function(error, job) {
			if (error) {
				return this.emit("error", error, "processJobs: find job");
			}
			
			if (job && this._status === "idle") {
				collection.findAndModify({"_id": job._id}, {"_id": 1}, {"$set": {"s": "p"}}, {"safe": true}, (function(error, upd) {
					if (error) {
						return this.emit("error", error, "processJobs: update job");
					};

					console.log("[processing] job " + job._id.toString());
					this._status = "processing";
					this._processing = upd;
					upd.o && this._broadcast(upd.o, {"command": "processJob", "params": upd});
				}).bind(this));
			};
		}).bind(this));
	}).bind(this));
};

Leeloo.prototype.reap = function() {
	this._store.collection(this._context.mongodb.collection, (function(error, collection) {
		if (error) {
			return this.handleError(error);
		};
		
		collection.remove({"s": "f"}, (function(error) {
			if (error) {
				return this.handleError(error);
			};
			console.log("[reaper] reaped");
			return;
		}).bind(this));
	}).bind(this));
};


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
	var callback = (params instanceof Function) ? params : callback;
	var params = (params instanceof Function) ? {} : params || {};
	var job = {};
	
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
	
	if (!job.xid) {
		return callback && callback(new Error("`xid` is a required parameter"));
	};
	
	this._store.collection(this._context.mongodb.collection, (function(error, collection) {
		if (error) {
			console.log(util.inspect(error, true, 7, true));
			return callback && callback();
		};

		collection.findAndModify(
				{"s": "s", "xid": job.xid, "t": job.t},
				{"s": 1, "w": 1},
				{"$set": job},
				{"safe": true, "multi": false, "upsert": true, "new": true},
				(function(error, doc) {
			if (error) {
				console.log(util.inspect(error, true, 7, true));
				return callback && callback();
			};
			
			this._broadcast(owner, {"command": "scheduledJob", "params": doc});
			return callback && callback();
		}).bind(this));
	}).bind(this));
};


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
};


/**
 * Starts Leeloo.
 *
 * @param {Function} callback `callback(error)` [optional]
 */
Leeloo.prototype.startServer = function(params, callback) {
	var net = require("net");
	var callback = (params instanceof Function) ? params : callback;
	var params = (params instanceof Function) ? {} : params || {};
	var port = params.port || 1997;
	
	this._openDatabase((function(error) {
		if (error) {
			this.handleError(error);
			return callback && callback();
		};
		console.log("[startup] connected to the database");
		this._started = Date.now();
		this.heartbeat_interval = setInterval((this._heartbeat).bind(this), params.heartbeat || 60000);
		
		if (params.reap_enabled) {
			console.log("[startup] reaper enabled");
			this.reap_interval = setInterval((this.reap).bind(this), params.reap_timer || 600000);
		}
		else {
			console.log("[startup] reaper disabled");
		};
		
		console.log("[startup] starting leeloo on port " + port);
		this._server = net.createServer({"allowHalfOpen": true}, (this.handleSocket).bind(this)).listen(port);
		this._server.on("error", (this.handleError).bind(this));
		
		// process commands in a queue as they are received
		this.on("processCommand", (this.processCommand).bind(this));
		
		callback && callback();
	}).bind(this));
};



/**
 * Stops Leeloo. Temporarily.
 *
 * @param {Function} callback `callback(error)` [optional]
 */
Leeloo.prototype.stopServer = function(params, callback) {
	this.heartbeat_interval = clearInterval(this.heartbeat_interval);
	if (this.reap_interval) {
		this.reap_interval = clearInterval(this.reap_interval);
	};
	this._store.close();
	this._server.close();
	
	callback && callback(null, "bye!");
	process.exit();
}

module.exports = new Leeloo;