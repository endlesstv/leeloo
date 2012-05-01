var Client = require("../index").Client,
	Server = require("../index").Server;

Server.startServer(function(error) {
	var client = new Client();
	
	client.on("connected", function() {
		console.log("Connected.");
		client.disconnect();
	});
	
	client.on("disconnected", function() {
		// You could put code here to poll and reconnect.
		console.log("Disconnected.");
		Server.stopServer(function(error) {
			process.exit();
		});
	});
	
	client.connect(1997);
});	