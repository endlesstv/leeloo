var leeloo = require("../lib/leeloo");

leeloo.connect(1997, function(error) {
	leeloo.dispatch("list", function(error, response) {
		leeloo.dispatch("schedule", function(error, response) {

			leeloo.disconnect(function(error) {

			});

		});		

	});
	
});