var events = require("events");
var net = require("net");
var util = require("util");

var LeelooClient = function() {
	this._connection = null;
	this.offline_queue = [];
	this.identified = false;
	this.enable_offline_queue = false;
	events.EventEmitter.call(this);
};

util.inherits(LeelooClient, events.EventEmitter);

/**
 * Connects to a Leeloo instance.
 *
 * @param {Integer} port The port Leeloo is running on
 * @param {String} host The host running Leeloo [optional]
 * @param {Function} callback `callback(error)` [optional]
 */
LeelooClient.prototype.connect = function(port, host, callback) {
	var data = "";
	var callback = (host instanceof Function) ? host : callback;
	var host = (host instanceof Function) ? "127.0.0.1" : host || "127.0.0.1";
	
	this._connection = net.createConnection(port, host);
	this._connection.removeAllListeners();	
	this._connection.setEncoding("utf8");
	this._connection.setKeepAlive(true);
	
	this._connection.on("connect", (function() {
		this.emit("connected");
		return callback && callback();
	}).bind(this));
	
	this._connection.on("data", (function(chunk) {
		data += chunk;
		var payloads = data.split("\n\n");
		data = "";

		if (payloads.length) {
			this._connection.pause();
			
			payloads.forEach((function(payload) {
				if (payload.slice(-1) === "}") {
					try {						
						payload = JSON.parse(payload);
						payload.command && this.emit(payload.command, payload.params || {});
					}
					catch (ex) {
						data = payload;
						this._connection.resume();
						ex.payload = payload;
						this.emit("error", ex);
					}
				}
				else if (payload.length) {
					data = payload;
					this._connection.resume();
				}
				else {
					this._connection.resume();
				};
			}).bind(this));
		};
	}).bind(this));
		
	this._connection.on("end", (function() {
		this.emit("disconnected"); 
	}).bind(this));
	
	this._connection.on("close", (function(had_error) { 
	}).bind(this));
	
	this._connection.on("drain", (function() {
	}).bind(this));
	
	this._connection.on("error", (function(error) {
		return this.emit("error", error);
	}).bind(this));
}


/**
 * Ends the current connection to Leeloo.
 *
 * @param {Function} callback `callback(error)` [optional]
 */
LeelooClient.prototype.disconnect = function(callback) {
	this._connection && this._connection.end();	
	return callback && callback();
};


/**
 * Commands Leeloo to do something. Doesn't wait for a response.
 *
 * @param {String} command The command to issue
 * @param {String} params Additional parameters [optional]
 * @param {Function} callback `callback(error)` [optional]
 */
LeelooClient.prototype.dispatch = function(command, params, callback) {
	var callback = (params instanceof Function) ? params : callback;
	var params = (params instanceof Function) ? {} : params || {};
	var payload = JSON.stringify({"command": command, "params": params}).replace(/\n+/g, "\n") + "\n\n";
	
	// if the connection is writeable, send the payload
	if (this._connection && this._connection.writable) {
		this._connection.write(payload);
		if (command === "identifyClient") {
			this.identified = true;
		};
		// empty the offline_queue only after we've sent identification to the client
		while (this.enable_offline_queue && this.identified && this.offline_queue.length) {
			this._connection.write(this.offline_queue.shift());
		};
	}
	else {
		// should handle `identification = false` on disconnect but i'm not there yet
		this.identified = false;
		if (this.enable_offline_queue) {
			this.offline_queue.push(payload);
		};
	};
	
	return callback && callback();
};

LeelooClient.prototype.identify = function(identity) {
	return this.dispatch("identifyClient", {"identity": identity});
};

module.exports = LeelooClient;