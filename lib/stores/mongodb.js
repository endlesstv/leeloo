var util = require("util");
var events = require("events");

var MongoStore = function() {
	var options = (arguments && arguments[0]) || {};
	events.EventEmitter.call(this);
};

util.inherits(MongoStore, events.EventEmitter);

module.exports = MongoStore;