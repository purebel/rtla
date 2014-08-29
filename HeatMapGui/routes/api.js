var express = require('express');
var dclient = require('../lib/listdb');
var router = express.Router();

router.get('/mac_list', function(req, res) {
    function _handler(err, data) {
	var result = [];
	if (data) {
	    var _item = data["Items"];
	    for (var _ele in _item) {
//		result.push({
//		    district: _item[_ele]["district"]["S"],
//                    cnt:  _item[_ele]["cnt"]["S"]
//		});
	    }
	}
        result.push({district: "2014-08-14"});
	res.json(result);
    }
    var _result = dclient.scan({
//	TableName: "QH_mac_day_mapping"
        TableName: "QH_heatmap2"
    }, _handler);
});

router.get('/heatmap_details', function (req, res) {
	console.log(req.query);
    function _handler(err, data) {
	console.log(err);
	var result = [];
        var sum=0;
        var hit_idx=0;
        var coorArr = [];
        var _coorx;
        var _coory;
	if (data) {
	    console.log(data);
	    var _item = data["Items"];
	    console.log(_item);
	    for (var _ele in _item) {
                sum=0;
                hit_idx=0
                for (var _exist_item in result){
                  if(result[_exist_item].district==_item[_ele]["district"]["S"]) {
                    sum=result[_exist_item].cnt;
                    break;
                  }
                  hit_idx++;
                }
                if(sum==0) {
console.log("new entry");
                  _district=_item[_ele]["district"]["S"];
                  coorArr=_district.split("-");
                  _coorx=coorArr[0];
                  _coory=coorArr[1];
		  result.push({
		      district: _district, //_item[_ele]["district"]["S"],
                      coorx: _coorx,
                      coory: _coory,
		      cnt: _item[_ele]["cnt"]["S"]
		  });
                }else{
                  result[hit_idx].cnt=sum+_item[_ele]["cnt"]["S"];
                }
	    }
	}
console.log("final result is:");
console.log(result);
	res.json(result);
    }
    dclient.query({
	TableName: "QH_heatmap2",
	KeyConditions: {

/*	    district: {
		ComparisonOperator: "EQ",
		AttributeValueList: [{
		    S: req.query.district
		}]
	    },
*/
            ts: {
                ComparisonOperator: "EQ",
                AttributeValueList: [{
                    S: req.query.ts
 	        }]
            }
/*
            ts: {
                ComparisonOperator: "BETWEEN",
                AttributeValueList: [
                   { S: req.query.tsMin},
                   { S: req.query.tsMax}
                ]
            }
*/
	},
    }, _handler);
});


module.exports = router;
