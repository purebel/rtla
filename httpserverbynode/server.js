var http = require('http');
var url = require('url');

function start(route, handle)
{
    function onRequest(request, response)
    {
        //var method = request.method;
        //var pathname = url.parse(request.url).pathname;
        //var postData = "";
        //console.log(method + " " + pathname + "  " + postData);
        route(handle, request, response);
        //console.log(request.headers);
        //console.log('[HTTP-Server] Request received with method[%s]', method);
        /*
        request.setEncoding("utf8");
        request.addListener("data", function(postDataChunk) {
            postData += postDataChunk;
            console.log("Received post data chunk " + postDataChunk + "..");
        });
        request.addListener("end", function(){
            console.log("[HTTP-Server] The whole request is:" + postData);
        });
        //route(handle, pathname, response);
        */
        response.writeHead(404, {"Content-Type": "text/plain"});
        response.end();
    }
    http.createServer(onRequest).listen(8080);
    console.log(__filename + ':[HTTP-Server] Server running at http://127.0.0.1:8080/');
}

exports.start = start;
