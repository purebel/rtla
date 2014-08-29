var AWS = require('aws-sdk');

AWS.config.update({
    region: 'us-east-1',
    accessKeyId : 'AKIAID5EQMPJUSWH3FXQ',
    secretAccessKey: '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'
});

var dynamodb = new AWS.DynamoDB();
var params = {
    TableName: "MSE",
};

dynamodb.scan(params, function(err, data) {
    for (var _da in data["Items"]) {
	dynamodb.deleteItem({
	    Key: {
		mac: {
		    S: data["Items"][_da]["mac"]["S"]
		},
		ts: {
		    S: data["Items"][_da]["ts"]["S"]
		}
	    },
	    TableName: "MSE",
	    
	}, function(err, data) {
	    if (err) console.log(err, err.stack); // an error occurred
	    else     console.log(data);  
	});
//	console.log(data["Items"][_da]);
    }
});
