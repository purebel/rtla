var AWS = require('aws-sdk');

AWS.config.update({
    region: 'us-east-1',
    accessKeyId : 'AKIAID5EQMPJUSWH3FXQ',
    secretAccessKey: '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'
});

var dynamodb = new AWS.DynamoDB();
var params = {
    TableName: "MSE",
    KeyConditions: {
	mac: {
	    ComparisonOperator: "EQ",
	    AttributeValueList: [{
		S: "34:e2:fd:3a:91:71"
	    }]
	}
    }
};
    dynamodb.query(params, function(err, data) {
	console.log(data);
    });
