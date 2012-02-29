var leeloo = require("../lib/leeloo");

leeloo.connect(1997, function(error) {

	//leeloo.dispatch("scheduleJobs", [{"w": Date.now() + 10000},{"w": Date.now() + 20000, "xid": 2},{"w": Date.now() + 30000}]);
	leeloo.dispatch("schedule", {"when": Date.now() + 1000, "reschedule": 1000, "xid": 1});

	leeloo.on("processJob", function(job) {
		console.log("Received a finish job for " + job._id);
		leeloo.dispatch("finishJob", {"id": job._id});					
	});
});