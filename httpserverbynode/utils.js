function serialize(obj)
{
    switch(obj.constructor)
    {
        case Function:
            break;
        case Object:
            var str = '{';
            for(var o in obj)
            {
                str += '\"' + o + '\"' + ':' + serialize(obj[o]) + ',';
            }
            if(str.substr(str.length - 1) == ',')
            {
                str = str.substr(0, str.length - 1);
            }
            return str + '}';
        case Array:
            var str = '[';
            for(var o in obj)
            {
                if(serialize(obj[o]) != undefined)
                    str += serialize(obj[o]) + ',';
            }
            if(str.substr(str.length - 1) == ',')
            {
                str = str.substr(0, str.length - 1);
            }
            return str + ']';
        case Boolean:
            return '\"' + obj.toString() + '\"';
        case Date:
            return '\"' + obj.toString() + '\"';
        case Number:
            return '\"' + obj.toString() + '\"';
        case String:
            return '\"' + obj.toString() + '\"';
        default:
            return; 
    }

}

exports.serialize = serialize;
