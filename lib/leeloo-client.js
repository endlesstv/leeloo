var events = require("events"),
	net = require("net"),
	util = require("util");

var LeelooClient = module.exports = function() {
	this._connection = null;
	
	events.EventEmitter(this);
}

util.inherits(LeelooClient, events.EventEmitter);

/**
 * Connects to a Leeloo instance.
 *
 * @param {Integer} port The port Leeloo is running on
 * @param {String} host The host running Leeloo [optional]
 * @param {Function} callback `callback(error)` [optional]
 */
LeelooClient.prototype.connect = function(port, host, callback) {
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
			data += chunk;
			var payloads = data.split("\n\n");
			data = "";

			if (payloads.length) {
				self._connection.pause();
				
				
				payloads.forEach(function(payload) {
					if (payload.slice(-1) === "}") {
						try {						
							payload = JSON.parse(payload);
							payload.command && self.emit(payload.command, payload.params || {});
						}
						catch (ex) {
							data = payload;
							self._connection.resume();
							ex.payload = payload;
							self.emit("error", ex);			
						}
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
	});
		
	self._connection.on("end", function() {
		self.emit("disconnected"); 
	});
	
	self._connection.on("close", function(had_error) { 
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
	var payload = {
		"command": command,
		"params": params
	};
	
	// if the connection is writeable, send the payload
	this._connection 
			&& this._connection.writable 
			&& this._connection.write(JSON.stringify(payload).replace(/\n+/g, "\n") + "\n\n");
			
	return callback && callback();
};
