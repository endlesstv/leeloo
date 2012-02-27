var leeloo = require("../lib/leeloo");

leeloo.connect(1997, function(error) {

	leeloo.dispatch("scheduleJobs", [{"w": Date.now() + 10000},{"w": Date.now() + 20000},{"w": Date.now() + 30000}]);

	leeloo.on("scheduledJobs", function(jobs) {
		console.log(jobs);
	});

});