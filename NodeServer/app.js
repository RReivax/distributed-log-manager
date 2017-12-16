var http = require('http');
var fs = require('fs');

var kafka = require('kafka-node');
var zookeeper = require('node-zookeeper-client');
var ConsumerGroup = require('kafka-node').ConsumerGroup;

/* 
 * Kafka Configuration
 */

var consumerOptions = {
	host: 'm1.adaltas.com:2181',
	kafkaHost: 'm1.adaltas.com:6667',
	groupId: 'xg-node-consumer'
}
var topics = ['xg_formatted']
var consumerGroup = new ConsumerGroup(consumerOptions, topics);

/*
 * NodeJs Server creation
 */

var server = http.createServer(function(req, res) {
	fs.readFile('./index.html', 'utf-8', function(error, content) {
		res.writeHead(200, {"Content-Type": "text/html"});
		res.end(content);
	});
});

/*
 * SocketIO 
 */

var io = require('socket.io').listen(server);
io.sockets.on('connection', function (socket) {
	console.log('Client connected.');
	socket.emit('message', 'Connection successful');
	
	/*	
	 * Log from Kafka handling
	 */
	consumerGroup.on('message', function (message) {
		socket.emit('log', message);
	});
});


server.listen(80);
