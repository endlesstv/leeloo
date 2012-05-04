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

##License

Copyright (c) 2012 Tip or Skip, Inc.

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
