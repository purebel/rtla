var express = require('express');
var path = require('path');
var favicon = require('static-favicon');
var logger = require('morgan');
var cookieParser = require('cookie-parser');
var bodyParser = require('body-parser');
var socketio = require('socket.io');

var routes = require('./routes/index');
var users = require('./routes/users');

var app = express();
var httpserver = require('http').Server(app);
var io = socketio(httpserver);
var kinesis = require('./lib/kinesis');

var page_sockets = [];
var all_data = [];

// view engine setup
app.set('views', path.join(__dirname, 'views'));
app.set('view engine', 'jade');

app.use(favicon(__dirname + '/public/'));
app.use(logger('dev'));
app.use(bodyParser.json());
app.use(bodyParser.urlencoded());
app.use(cookieParser());
app.use(require('less-middleware')(path.join(__dirname, 'public')));
app.use(express.static(path.join(__dirname, 'public')));

app.use('/', routes);
app.use('/users', users);

/// catch 404 and forward to error handler
app.use(function(req, res, next) {
    var err = new Error('Not Found');
    err.status = 404;
    next(err);
});

/// error handlers

// development error handler
// will print stacktrace
if (app.get('env') === 'development') {
    app.use(function(err, req, res, next) {
        res.status(err.status || 500);
        res.render('error', {
            message: err.message,
            error: err
        });
    });
}

// production error handler
// no stacktraces leaked to user
app.use(function(err, req, res, next) {
    res.status(err.status || 500);
    res.render('error', {
        message: err.message,
        error: {}
    });
});

//Socket.IO server
io.of('/kinesis-io').on('connection', function(socket) {
    socket.on('news', function(data) {
	if (data.length > 13) {
	    var new_data = JSON.parse(data.substr(13));
	    new_data['X'] = parseFloat(new_data['X']);
	    new_data['Y'] = parseFloat(new_data['Y']);
	    new_data['size'] = 1;
	    console.log(new_data);
	    all_data.push(new_data);
	    page_io.emit('dataReceived', new_data);
	}

    });
});
var page_io = io.of('/page-io');
page_io.on('connection', function(socket) {
    all_data.forEach(function(ele, indx) {
	page_io.emit('dataReceived', ele);
    });
});
kinesis.request.send();
module.exports = {
    app: app,
    server: httpserver
};

