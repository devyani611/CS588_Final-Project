db.loopdata.aggregate(
  [
	{
  	$match: {
    	speed: {
      	$gt: 100
    	}
  	}
	},
	{
  	$count: "great_speed"
	}
  ]
).explain('executionStats');
