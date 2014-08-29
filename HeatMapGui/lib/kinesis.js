/*
 * 
 *
**/

var AWS = require('aws-sdk');
var kinesis_io = require('socket.io-client')('http://localhost:8001/kinesis-io');
AWS.config.update({
    region: 'us-east-1',
    accessKeyId : 'AKIAID5EQMPJUSWH3FXQ',
    secretAccessKey: '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'
});
var kinesis = new AWS.Kinesis();
//Get all shards in the stream
var exclusiveStartShardId = null;
var desParams = {
    StreamName: 'RTLocation2', // required
    ExclusiveStartShardId: null,
    Limit: 1
};
var shards = [];
var describeRequest = kinesis.describeStream(desParams);
var processRecord = function(data) {
    console.log(data);
    if (data.length > 0) {
	data.forEach(function(record, index, arr) {
	    kinesis_io.emit('news', record.Data.toString());
	});
    }
};
var getRecord = function(params) {
    kinesis.getRecords({
	ShardIterator: params,
    }, function(err, data) {
	if (err) {
	    console.log(err);
	    throw new Error("Get record error");
	} else {
	    processRecord(data.Records);
	    if (data.NextShardIterator) {
		setTimeout(function() {
		    getRecord(data.NextShardIterator);
		}, 1000);
	    }
	}
    });
};

describeRequest.on("success", function(resp) {
    var data = resp.data;
    shards = shards.concat(data.StreamDescription.Shards);
    if (data.StreamDescription.HasMoreShards && shards.length > 0) {
	exclusiveStartShardId = shards[shards.length - 1].ShardId;
	desParams.ExclusiveStartShardId = exclusiveStartShardId;
	describeRequest.send();
    } else if (shards.length > 0) {
	exclusiveStartShardId = null;

	shards.forEach(function(shard, index, arr) {
	    console.log(shard);
	    kinesis.getShardIterator({
		ShardId: shard.ShardId,
		StreamName: desParams.StreamName,
		ShardIteratorType: "TRIM_HORIZON"
	    }, function(err, shardIterator) {
		if (err) {
		    console.log(err);
		    throw new Error("Get shard Iterator error");
		} else {
		    getRecord(shardIterator.ShardIterator);
		}
	    });

	});
    } else {
	exclusiveStartShardId = null;
    }
}).on("error", function(err) {

}).on("complete", function(resp) {

});
module.exports = {
    request: describeRequest
};
