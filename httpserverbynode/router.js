var url = require('url');

function route(handle, request, response)
{
    var method = request.method;
    var pathname = url.parse(request.url).pathname;
    pathname = pathname + method.toLowerCase();

    console.log(__filename + '[HTTP-Server] Received %s request.', method);

    if(typeof handle[pathname] === 'function')
    {
        handle[pathname](request, response);
    }
    else
    {
        response.writeHead(404, {"Content-Type": "text/plain"});
        response.write('404 Not found');
        response.end();
    }
}

exports.route = route;
