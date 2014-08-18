var io = io.connect('/page-io');
var draw;
var devices = {};
var raw_data = new kendo.data.DataSource();
function calcX(x) {
    var floatx = parseFloat(x), width = draw.width();
    return 30 + ((floatx - 50) / 70 * width);
}
function calcY(y) {
    var floaty = parseFloat(y), height = draw.height();
    return ((120 - floaty) / 70) * height;
}

function drawChart() {
    $("#chart").kendoChart({
        title: {
            text: "Real-time Device Nodes"
        },
        legend: {
            visible: false
        },
        dataSource: raw_data,
        series: [{
            type: "bubble",
            xField: "X",
            yField: "Y",
	    sizeField: "size",
            categoryField: "MAC",
	    maxSize: 10,
	    colorField: "color"
        }],
        xAxis: {
	    labels: {
                format: "{0:N0}",
            },
	    min: 0,
	    max: 120
        },
        yAxis: {
	    labels: {
                format: "{0:N0}",
	    },
	    min: 0,
	    height: 120
        },
	tooltip: {
            visible: true,
	    template: 'X: ${dataItem.X} - Y: ${dataItem.Y} - MAC: ${dataItem.MAC} - TimeStamp: ${dataItem.Timestamp}',
//            format: "{3}: {0}",
            opacity: 1
        },
	dataBound: function(e) {
	    clearTimeout(dataCacheTimeout);
	    dataCacheTimeout = setTimeout(function() {
		console.log("data bound");
		var _modelMap = $("#chart").data("kendoChart")._model.modelMap;
		var _mult = _.filter(devices, function(ele) {
		    return (ele.data.length > 1);
		});
		var _svg = $("#chart > svg");
		var _svgString = '<svg class="node-path">';
		_mult.forEach(function(ele, ind) {
		    var _color = ele.color,
		        _nodes = $('circle[fill="' + _color + '"]');
		    //draw the path
		    for (var i = 1; i < ele.data.length; i++) {
			var _preNode = ele.data[i-1], _curNode = ele.data[i];
			var _preCir = _.find(_nodes, function(_nd) {

			    _nd = $(_nd);
			    return (_modelMap[_nd.attr('data-model-id')].dataItem['X'] == _preNode['X'] && _modelMap[_nd.attr('data-model-id')].dataItem['Y'] == _preNode['Y']);
			}), _curCir = _.find(_nodes, function(_nd) {
			    _nd = $(_nd);
			    return (_modelMap[_nd.attr('data-model-id')].dataItem['X'] == _curNode['X'] && _modelMap[_nd.attr('data-model-id')].dataItem['Y'] == _curNode['Y']);
			});
			var _preCirCX = parseFloat($(_preCir).attr('cx')), _preCirCY = parseFloat($(_preCir).attr('cy')), _curCirCX = parseFloat($(_curCir).attr('cx')), _curCirCY = parseFloat($(_curCir).attr('cy'));
			var _delCX = _curCirCX - _preCirCX, _delCY = _curCirCY - _preCirCY;
			var _posCX = _delCX -  _delCX * 6 / Math.sqrt(_delCX * _delCX + _delCY * _delCY), _posCY = _delCY - _delCY * 6 / Math.sqrt(_delCX * _delCX + _delCY * _delCY);

			_svgString += '<marker id="' + $(_curCir).attr('id') + 'markerArrow" markerWidth="13" markerHeight="13" refx="2" refy="6"           orient="auto">        <path d="M2,2 L2,11 L10,6 L2,2" style="fill: ' + _color + '" />    </marker><path d="M ' + _preCirCX + ' ' + _preCirCY + ' l ' + _posCX + ' ' + _posCY + ' " stroke-width="1" stroke="' + _color + '" marker-end="url(#' + $(_curCir).attr('id') + 'markerArrow)" />';
			
		    }
		});
		_svgString += '</svg>';
		_svg.append(_svgString);
	    }, 3000);

	}
    });
}
var calcCount = 0;
function getRandomColor() {
    var letters = '0123456789ABCDEF'.split('');
    var color = '#';
    for (var i = 0; i < 6; i++ ) {
        color += letters[Math.floor(Math.random() * 16)];
    }
    return color;
}
var dataCacheTimeout = 0;

io.on('dataReceived', function(data) {
    if (data.MAC in devices) {
	devices[data.MAC].data.push(data);
    } else {
	devices[data.MAC] = {color: getRandomColor(), data: []};
	devices[data.MAC].data.push(data);
    }
    data['color'] = devices[data.MAC].color;
    raw_data.add(data);
});

$(function() {
/*    draw = SVG('svg-area').size(830, 420);
    draw.rect(800, 400).dmove(30, 0).stroke({width: 1, color: '#C7C7C7'}).fill('#FFFFFF');
    draw.text('50').y(400);
    draw.text('120').x(0);
    draw.text('120').y(400).x(800);*/
    
});

setTimeout(function() {
//    drawChart();
}, 100);
