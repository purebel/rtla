var kinesis_request = require('./kinesis');
var app = require('express')();
var httpserver = require('http').Server(app);
var io = require('socket.io')(httpserver);
httpserver.listen(8001)

kinesis_request.request.send();

