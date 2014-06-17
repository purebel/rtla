// A test server for HTTP post by Jason
var server = require('./server');
var router = require('./router');
var requesthandlers = require('./requesthandlers');

var handle = {}
handle['/post'] = requesthandlers.post;
handle['/get'] = requesthandlers.get;

server.start(router.route, handle);
