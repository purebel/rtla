var AWS = require('aws-sdk');

AWS.config.update({
    region: 'us-east-1',
    accessKeyId : 'AKIAID5EQMPJUSWH3FXQ',
    secretAccessKey: '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'
});

var dynamodb = new AWS.DynamoDB();
var params = {
    KeyConditions: {
	
    }
};