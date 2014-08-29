#!/usr/bin/env node
var debug = require('debug')('rtlaconsumer');
var app = require('./app');

app.server.listen(8001);

