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
var option = {
    title : {
        text: 'CBC_7F_HM',
        x:'right'
    },
    tooltip : {
        trigger: 'item',
        showDelay: 0,
        transitionDuration: 0.2,
        formatter : function (a) {
 //           var sName = a[0];
 //           var pName = a[1];
//            var value = a[2] + '';
//            value = value.replace(/(\d{1,3})(?=(?:\d{3})+(?!\d))/g, '$1,');
//            return sName + '<br/>' + pName + ' : ' + value;
              return a[0];
        }
    },
    dataRange: {
        x : 'right',
        min: 0,
        max: 20,
        color: ['orangered','yellow','lightskyblue'],
        text:['High','Low'],           // 文本，默认为数值文本
        calculable : true
    },
    toolbox: {
        show : true,
        //orient : 'vertical',
        x: 'left',
        y: 'top',
        feature : {
            mark : {show: true},
            dataView : {show: true, readOnly: false},
            restore : {show: true},
            saveAsImage : {show: true}
        }
    },
    series : [
        {
            name: 'DEMO',
            type: 'map',
            roam: true,
            mapType: 'baiduBuilding', // 自定义扩展图表类型
            itemStyle:{
                emphasis:{label:{show:true}}
            },
            // 文本位置修正
//            textFixed : {
//                Alaska : [20, -20]
//            },
            data:[
                {name : 'Alabama', value : 4822023}
            ]
        }
    ]
};  
                  

    echarts.util.mapData.params.params.baiduBuilding = {
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
*/    
/*
var placeListStrong = [
    {name:'海门', geoCoord:[1.15, 131.89], cnt: 0}
];
var placeListMedium = [
    {name:'海门', geoCoord:[1.15, 31.89], cnt: 0}
];
var placeListWeak = [
    {name:'海门', geoCoord:[1.15, 31.89], cnt: 0}
];
*/
    var option = {
	backgroundColor:'#eee',
	title : {
//            text : 'CBC_7F',
            text : 'CBC_7F_HM',
            textStyle: {
		color: '#000'
            }
	},
/*
	tooltip : {
            trigger: 'item',
         showDelay: 0,
        transitionDuration: 0.2,
           formatter: function(v) {
		return v[1];
            }
	},

	color: ['rgba(218, 70, 214, 1)', 'rgba(100, 149, 237, 1)', 'green'],
	legend: {
            data: ['DEMO']
	},
*/
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
//                    normal:{label:{show:true}},
//                    emphasis:{label:{show:true}}
		},

            data : [],
            markPoint : {
                symbolSize: 2,
                large: true,
                effect : {
                    show: true
                },
                data : [
                  {name: 'point', geoCoord:[25, 39], cnt: 100}
                ]
/*
                data : (function(){
                    var data = [];
                    var len = 3000;
                    var geoCoord
                    while(len--) {
                        geoCoord = placeListWeak[len % placeListWeak.length].geoCoord;
                        data.push({
                            name : placeListWeak[len % placeListWeak.length].name + len,
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
                data : [
                  {name: 'point', geoCoord:[25, 39], cnt: 100}
                ]
/*
                data : (function(){
                    var data = [];
                    var len = 1000;
                    var geoCoord
                    while(len--) {
                        geoCoord = placeListMedium[len % placeListMedium.length].geoCoord;
                        data.push({
                            name : placeListMedium[len % placeListMedium.length].name + len,
                            value : 50,
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
                data : [
                  {name: 'point', geoCoord:[25, 39], cnt: 100}
                ]
/*
                data : (function(){
                    var data = [];
                    var len = placeListStrong.length;
                    while(len--) {
                        data.push({
                            name : placeListStrong[len].name,
                            value : 90,
                            geoCoord : placeListStrong[len].geoCoord
                        })
                    }
                    return data;
                })()
*/
            }
/*
		data: [],
		geoCoord: {
                    '叮叮': [39, 45],
                    '小兑儿': [71, 45],
		},
		markPoint : {
                    symbolSize : 3,
                    data : [
			{name: '叮叮'},
			{name: '小兑儿'},
                    ]
		},

		markLine : {
                    smooth:false, //true,
                    effect : {
			show: false,//true,
			scaleSize: 1,
			period: 20,
			color: '#fff',
			shadowBlur: 5
                    },
                    symbol: ['none'],
                    itemStyle : {
			normal: {
                            borderWidth:1,
                            lineStyle: {
				type: 'solid'
                            }
			}
                    },

                    data : [
			[
                            {name:'叮叮'}, 
                            {name:'小兑儿'}
			]
                    ]//data
		}//markLine
*/
            }//series
	]//series
    };//option


    $("#dropdown").kendoDropDownList({
//	dataTextField: "mac",
//	dataValueField: "mac",
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
	$.ajax({
	    type: "GET",
//	    url: "/apis/mac_day_details",
            url: "/apis/heatmap_details",
	    data: _data,
	    dataType: "json",
	    success: function(data) {
		var _geoCoord = {};
		var _mp_data = [];
		var _ml_data = [];
                var _placeListWeak = [];
                var _placeListMedium = [];
                var _placeListStrong = [];
                var _coorx;
                var _coory;
                var _cnt;

		for (var _ele in data) {
		    if (_ele < 10) {
		    var _item = data[_ele];
console.log(_item);
                    _coorx=_item["coorx"] * 671 / 128+500;
                    _coory=_item["coory"] * 620 / 128+500;       
                    _cnt=_item["cnt"];
console.log(_cnt);
if(_cnt<2) {
                    _placeListWeak.push({name: _item["district"], geoCoord:[_coorx, _coory], cnt: _item["cnt"]});
}else if(_cnt < 5) {
 _placeListMedium.push({name: _item["district"], geoCoord:[_coorx, _coory], cnt: _item["cnt"]});
}else {
 _placeListStrong.push({name: _item["district"], geoCoord:[_coorx, _coory], cnt: _item["cnt"]});
}

console.log("start displaying _placeListWeak: "); 
console.log(_placeListWeak);
console.log("start displaying _placeListMedium: "); 
console.log(_placeListMedium);

		    if (_ele != 0) {
			_ml_data.push([
			    {name: data[_ele-1]["ts"]},
			    {name: data[_ele]["ts"]}
			]);
		    }

		    }
		}
             
//		option.series[0].geoCoord = _geoCoord;
//		option.series[0].markPoint.data = _mp_data;
//		option.series[0].markLine.data = _ml_data;
option.series[2].markPoint.data=_placeListStrong;
option.series[1].markPoint.data=_placeListMedium;
option.series[0].markPoint.data=_placeListWeak;
console.log("====== markPoint Medium =======");
console.log(option.series[1].markPoint.data);
/*placeListMedium=_placeListMedium;
placeListStrong=_placeListStrong;
console.log("----------start displaying placeListWeak: "); 
console.log(placeListWeak);
console.log("start displaying placeListMedium: "); 
console.log(placeListMedium);
*/
		myCharts.setOption(option);
	    }
	});

    });
});

setTimeout(function() {
    //    drawChart();
}, 100);
