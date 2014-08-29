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
    echarts.util.mapData.params.params.HK = {
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
    
    var option = {
	backgroundColor:'#eee',
	title : {
            text : 'CBC_7F',
            textStyle: {
		color: '#000'
            }
	},
	tooltip : {
            trigger: 'item',
            formatter: function(v) {
		return v[1];
            }
	},
	color: ['orangered','green','lightskyblue'],//['rgba(218, 70, 214, 1)', 'rgba(100, 149, 237, 1)', 'green'],
	legend: {
            data: ['DEMO']
	},
    dataRange: {
        x : 'left',
        min: 0,
        max: 7,
        color: ['orangered','green','lightskyblue'],
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
		name: 'DEMO',
		type: 'map',
		mapType: 'HK',
		roam:true,
		itemStyle:{
                    normal: {
                      borderColor:'rgba(100,149,237,1)',
                      borderWidth:1.5,
                      areaStyle:{
                        color: '#1b1b1b'
                      }
                    }
//                    normal:{label:{show:true}},
//                    emphasis:{label:{show:true}}
		},
		data: [],
		geoCoord: {
                    '叮叮': [139, 45],
                    '小兑儿': [71, 145],
		},
		markPoint : {
                    symbol : 'rectangle',
                    symbolSize : 3,
//                large: true,
//                effect : {
//                    show: false
//                },
                    data : [
			{name: '叮叮'},
			{name: '小兑儿'},
                    ]
		},
		markLine : {
                    smooth:true,
                    effect : {
			show: true,
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
                            {name:'叮叮', value: 1}, 
                            {name:'小兑儿', value: 1}
			]
                    ]
		}
            }
	]
    };
/*
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
*/
    $("#calendar").kendoCalendar({
	format: "yyyy/MM/dd",
	value: new Date(2014, 8, 13)
    });
    $("#calendar2").kendoCalendar({
	format: "yyyy/MM/dd",
	value: new Date(2014, 8, 14)
    });

    $("#apply").off("click").click(function(event) {
	var _cal = $("#calendar").data("kendoCalendar").value();
	var _cal2 = $("#calendar2").data("kendoCalendar").value();
	var _mp_data = [];
        var dateDelta=0;

        if(_cal.getMonth()<10) {
        	var _calstr = _cal.getFullYear() + "-" + "0" + (_cal.getMonth()) + "-" + _cal.getDate();
        }else{
        	var _calstr = _cal.getFullYear() + "-" + (_cal.getMonth()) + "-" + _cal.getDate();
        }

        if(_cal2.getMonth()<10) {
        	var _calEnd = _cal2.getFullYear() + "-" + "0" + (_cal2.getMonth()) + "-" + _cal2.getDate();
        }else{
        	var _calEnd = _cal2.getFullYear() + "-" + (_cal2.getMonth()) + "-" + _cal2.getDate();
        }       

        console.log(_calstr);
	console.log(_calEnd);

        var sArr = _calstr.split("-");
        var eArr = _calEnd.split("-");
 
        var sDate= new Date(sArr[0], sArr[1], sArr[2]);
        var eDate= new Date(eArr[0], eArr[1], eArr[2]);
        var dateDelta=(eDate-sDate)/(24*60*60*1000); 

        console.log("dateDelta:");
        console.log(dateDelta);

for(var i=0; i<=parseInt(dateDelta); i++) { 
        var newDate = new Date(Number(sArr[0]), Number(sArr[1]), Number(sArr[2])+i);
	console.log("newDate:");
	console.log(newDate);
	console.log("newDate.month");
	console.log(newDate.getMonth());
        if(newDate.getMonth()<10) {
          var queryDate=newDate.getFullYear() + "-0" + (newDate.getMonth()) + "-" +newDate.getDate();
        }else{
          var queryDate=newDate.getFullYear() + "-" + (newDate.getMonth()) + "-" +newDate.getDate();
        }
        console.log(queryDate);

	var _data = {
	     ts: queryDate
	};

	console.log("query key:");
	console.log(_data.ts);
	$.ajax({
	    type: "GET",
            url: "/apis/heatmap_details",
	    data: _data,
	    dataType: "json",
	    success: function(data) {
		var _geoCoord = {};
		var _ml_data = [];
                var _coorx;
                var _coory;
                var _cnt;
                var sum=0;
                var hit=0;
                var hit_idx=0;

		for (var _ele in data) {
		    var _item = data[_ele];
		    console.log(_item);
                    _coorx=_item["coorx"] * 671 / 128;
                    _coory=_item["coory"] * 620 / 128; 
                    _cnt=_item["cnt"];
		    console.log("x:y");
		    console.log(_coorx);
		    console.log(_coory);
		    console.log(_cnt);
                    _geoCoord[_item["district"]]=[_coorx,_coory];
//		    _geoCoord[_item["ts"]] = [parseFloat(_item["coorx"]) * 671 / 128, (parseFloat(_item["coory"])) * 620 / 128];

       		    console.log("incoming district=");
       		    console.log(_item["district"]);
       		    hit_idx=0;
  		    hit=0;
  		    sum=0;
  		    for(var _exist_item in _mp_data) {
  		       console.log("current district=");
  		       console.log(_mp_data[_exist_item].name);
  		       if(_mp_data[_exist_item].name ==_item["district"]) {
  		         sum=parseInt(_mp_data[_exist_item].value)+parseInt(_cnt);
  		         hit=1;
  		         break;
  		       }
  		       hit_idx++;
  		    }
                    if(hit==0){
                        console.log("search result:");
                        console.log(hit);
                        console.log(sum);
                        console.log(hit_idx);
                 
		        _mp_data.push({
//		            name: _item["ts"]
                            name: _item["district"],
                            value:_cnt
		        });
                    } else{//hit
                        console.log("search result:");
                        console.log(hit);
                        console.log(sum);
                        console.log(hit_idx);
                      _mp_data[hit_idx].value=sum;
                    }

		}//for
		option.series[0].geoCoord = _geoCoord;
		option.series[0].markPoint.data = _mp_data;
		option.series[0].data = _mp_data;
		option.series[0].markLine.data = _ml_data;
		myCharts.setOption(option);
	    }
	});//aj
}
    });
});

setTimeout(function() {
    //    drawChart();
}, 100);
