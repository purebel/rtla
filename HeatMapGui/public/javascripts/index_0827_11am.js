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
var myCharts;

$(function() {
    $("#nav-bar").kendoMenu();
    var myCharts = echarts.init(document.getElementById('content-map'));

    echarts.util.mapData.params.params.USA = {
	getGeoJson: function (callback) {
            $.ajax({
		url: "images/CBC_7F.svg",
		dataType: 'xml',
		success: function(xml) {
                    callback(xml)
		}
            });
	}
    };

/*
var placeListStrong = [
    {name:'海门', coor:[1.15, 331.89], cnt: 0}
];
var placeListMedium = [
    {name:'海门', geoCoord:[1.15, 231.89], cnt: 0}
];
var placeListWeak = [
    {name:'海门', geoCoord:[1.15, 131.89], cnt: 0}
];
*/
    $("#calendar").kendoCalendar({
	format: "yyyy/MM/dd",
	value: new Date(2014, 8, 13)
    });
    $("#apply").off("click").click(function(event) {
	var _cal = $("#calendar").data("kendoCalendar").value();
if(_cal.getMonth()<10) {
	var _calstr = _cal.getFullYear() + "-" + "0" + (_cal.getMonth()) + "-" + _cal.getDate();
}else{
	var _calstr = _cal.getFullYear() + "-" + (_cal.getMonth()) + "-" + _cal.getDate();
}
	var _mac = $("#dropdown").data("kendoDropDownList").value();
	var _data = {
	    ts: _calstr,
	    district: _mac
	};

                var placeListWeak = [];
                var placeListMedium = [];
                var placeListStrong = [];
	$.ajax({
	    type: "GET",
            url: "/apis/heatmap_details",
	    data: _data,
	    dataType: "json",
	    success: function(data) {
		var _geoCoord = {};
		var _mp_data = [];
		var _ml_data = [];

                var _coorx;
                var _coory;
                var _cnt;

		for (var _ele in data) {
		    var _item = data[_ele];
console.log(_item);
                    _coorx=_item["coorx"] * 671 / 128+500;
                    _coory=_item["coory"] * 620 / 128+500;       
                    _cnt=_item["cnt"];
console.log(_cnt);
if(_cnt<2) {
                    placeListWeak.push({name: _item["district"], value: 10, geoCoord:[_coorx, _coory], cnt: _item["cnt"]});
}else if(_cnt < 5) {
 placeListMedium.push({name: _item["district"], value: 50, geoCoord:[_coorx, _coory], cnt: _item["cnt"]});
}else {
 placeListStrong.push({name: _item["district"], value: 90, geoCoord:[_coorx, _coory], cnt: _item["cnt"]});
}
}

option.series[2].markPoint.data=placeListStrong;
option.series[1].markPoint.data=placeListMedium;
option.series[0].markPoint.data=placeListWeak;
console.log("====== markPoint Medium =======");
console.log(option.series[1].markPoint.data);

console.log("----------start displaying placeListWeak: ------- "); 
console.log(placeListWeak);
console.log("start displaying placeListMedium: "); 
console.log(placeListMedium);

		myCharts.setOption(option);
	    }
	});

    });


    var option = {
	backgroundColor:'#eee',
	title : {
//            text : 'CBC_7F',
            text : 'CBC_7F_HM',
            textStyle: {
		color: '#000'
            }
	},
    color: [
        'rgba(255, 125, 125, 0.8)',
        'rgba(14, 241, 242, 0.8)',
        'rgba(37, 140, 249, 0.8)'
    ],

    legend: {
        orient: 'vertical',
        x:'left',
        data:['强','中','弱'],
        textStyle : {
            color: '#fff'
        }
    },

    dataRange: {
        x : 'right',
        min: 0,
        max: 10,
        color: ['orangered','yellow','lightskyblue'],
        text:['High','Low'],           // 文本，默认为数值文本
        calculable : true
    },

	toolbox: {
            show : false,
            feature : {
		mark : {show: false},
		dataView : {show: true, readOnly: true},
		restore : {show: false},
		saveAsImage : {show: false}
            }
	},
	series : [
            {
		name: '弱',
		type: 'map',
		mapType: 'USA',
		roam:true,
		itemStyle:{
                normal:{
                    borderColor:'rgba(100,149,237,1)',
                    borderWidth:1.5,
                    areaStyle:{
                        color: '#1b1b1b'
                    }
                }//normal

	        }, //itemStyle

            data : [],
geoCoord: {
'1-1':10
},
            markPoint : {
                symbolSize: 2,
                large: true,
                effect : {
                    show: true
                },
data:[
{name:'1-1'}
]
/*
                data : (function(){
                    var data = [];
                    var len = 3000;
                    var geoCoord
                    while(len--) {
                        geoCoord = placeList[len % placeList.length].geoCoord;
                        data.push({
                            name : placeList[len % placeList.length].name + len,
                            value : 10,
                            geoCoord : [
                                geoCoord[0], // + Math.random() * 5 * -1,
                                geoCoord[1]// + Math.random() * 3 * -1
                            ]
                        })
                    }

                    return data;
                })()
*/
            }
        },
        {
            name: '中',
            type: 'map',
            mapType: 'USA',
            data : [],
            markPoint : {
                symbolSize: 3,
                large: true,
                effect : {
                    show: true
                },

                data : (function(){
                    var data = [];
                    var len = 1000;
                    var geoCoord
                    while(len--) {
                        geoCoord = placeList[len % placeList.length].geoCoord;
                        data.push({
                            name : placeList[len % placeList.length].name + len,
                            value : 50,
                            geoCoord : [
                                geoCoord[0], // + Math.random() * 5 * -1,
                                geoCoord[1]// + Math.random() * 3 * -1
                            ]
                        })
                    }
                    return data;
                })()

            }
        },
        {
            name: '强',
            type: 'map',
            mapType: 'USA',
            hoverable: false,
            roam:true,
            data : [],
            markPoint : {
                symbol : 'diamond',
                symbolSize: 6,
                large: true,
                effect : {
                    show: true
                },

                data : (function(){
                    var data = [];
                    var len = placeListStrong.length;
                    while(len--) {
                        data.push({
                            name : placeList[len].name,
                            value : 90,
                            geoCoord : placeList[len].geoCoord
                        })
                    }
                    return data;
                })()

            }
            }//series
	]//series
    };//option


    $("#dropdown").kendoDropDownList({
	dataTextField: "district",
	dataValueField: "district",
	dataSource: {
	    transport: {
		read: {
		    dataType: "json",
		    url: "/apis/mac_list"
		}
	    }
	}
    });


});

setTimeout(function() {
    //    drawChart();
}, 100);
