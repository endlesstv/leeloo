/**
 * "Multi-pass!"
 * "Yes, dear, she knows it's a multipass."
 */
var events = require("events"),
	net = require("net"),
	util = require("util");
	
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
			console.log("\tstart-server");
			console.log("\tstop-server");
	}
	
	callback && callback();
}


/**
 * Starts Leeloo.
 *
 * @param {Function} callback `callback(error)` [optional]
 */
Leeloo.prototype.startServer = function(callback) {
	var self = this;
	
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
			console.log(data);
			self[data] && self[data](function(error, message) {
				socket.end(message);
			});
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
}


/**
 * Stops Leeloo. Temporarily.
 *
 * @param {Function} callback `callback(error)` [optional]
 */
Leeloo.prototype.stopServer = function(callback) {
	var self = this;
	
	self._server.close();
	
	callback && callback(null, "bye!");
}

module.exports = new Leeloo;