var fs = require('fs');
var buffer = require('buffer');
var path = require('path');

function processPost(request, response)
{
    var postData = "";
    request.setEncoding("utf8");

    request.addListener("data", function(postDataChunk) {
        postData += postDataChunk;
        console.log(__filename + "[HTTP-Server] Received ata chunk %s with length %d.", postDataChunk, postDataChunk.length);
    });

    request.addListener("end", function(){
        console.log(__filename + "[HTTP-Server] The post payload is: %s \n", postData);
    });

    response.writeHead(200, {"Content-Type": "text/plain"});
	response.end();
}

exports.post = processPost;
