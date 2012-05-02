#Leeloo

Leeloo is a job scheduling server written in Node.js using a MongoDB database to maintain and run routine tasks. Commands can currently be issued through the command line or with `require("leeloo")` after a local install. I'll be cleaning up the client/server interface at some point.

'Leeloo' is also a possible phonetic spelling for 'LILO', which computer science nerds may know means 'Last In Last Out'; a queue.

##Server

##Client

```
var LeelooClient = require("leeloo").Client,
	leeloo_client = new LeelooClient();

// Connect to a Leeloo server at localhost:12345
leeloo_client.connect(12345);


// Identify the client to the server
leeloo_client.on("connected", function() {
	leeloo_client.dispatch("identifyClient", {"identity": "myApp"});
	leeloo_client.dispatch("schedule", )
});


// Process a job; send a cancel or finished message back.
leeloo_client.on("processJob", function(job) {
	switch (job.t) {
		case "emailMeSomething":
			emailMeSomething();
			leeloo_client.dispatch("finishJob", {"id": job._id});			
			break;
		
		case "updateThisThing":
			updateThisThing();
			leeloo_client.dispatch("finishJob", {"id": job._id});			
			break;
		
		default:
			mysteryJobIsMysterious();
			leeloo_client.dispatch("cancelJob", {"id": job._id});
	}
});

leeloo_client.on("disconnect", function() {
	var reconnect = function() {
		leeloo_client.connect(12345);
	}
	
	setTimeout(reconnect, 10000);
});
```

